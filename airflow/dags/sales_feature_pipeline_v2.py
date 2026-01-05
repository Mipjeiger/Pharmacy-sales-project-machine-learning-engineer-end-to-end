"""
Sales Feature Pipeline - Simplified version without multiprocessing conflicts
This DAG reads from Kafka, processes data through Bronze -> Silver -> Gold layers
"""

import io
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_minio_client():
    """Create MinIO client - called inside each task"""
    from minio import Minio
    from dotenv import load_dotenv

    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    env_path = os.path.join(project_root, ".env")
    load_dotenv(dotenv_path=env_path)

    return Minio(
        f"{os.getenv('minio_host')}:{os.getenv('minio_port')}",
        access_key=os.getenv("access_key"),
        secret_key=os.getenv("secret_key"),
        secure=False,
    )


def get_db_engine():
    """Create database engine - called inside each task"""
    from sqlalchemy import create_engine
    from dotenv import load_dotenv

    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    env_path = os.path.join(project_root, ".env")
    load_dotenv(dotenv_path=env_path)

    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def kafka_to_bronze():
    """Read data from Kafka and save to MinIO bronze bucket using consumer.py"""
    import sys

    # Add project root to path to import consumer module
    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Import read_batch from kafka.consumer
    # Use importlib to avoid conflict with kafka-python package
    import importlib.util

    consumer_path = os.path.join(project_root, "kafka", "consumer.py")
    spec = importlib.util.spec_from_file_location(
        "kafka_consumer_module", consumer_path
    )
    consumer_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(consumer_module)

    # Get functions from module
    read_batch = consumer_module.read_batch

    minio = get_minio_client()
    count = 0

    print("Starting to consume messages from Kafka using consumer.py...")

    try:
        # Use read_batch with max_batches to limit files created
        for batch in read_batch(limit=1000, max_batches=5):
            if not batch:
                print("Empty batch received, skipping...")
                continue

            print(f"Received batch of {len(batch)} messages")

            # Convert batch to DataFrame
            df = pd.DataFrame(batch)

            # Save to MinIO as parquet
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            file_name = f"pharmacy_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{count}.parquet"
            minio.put_object("bronze", file_name, buffer, len(buffer.getbuffer()))
            print(f"✓ Saved {len(df)} records to bronze/{file_name}")

            count += 1

        print(f"✓ Total files created: {count}")

    except Exception as e:
        print(f"Error in kafka_to_bronze: {e}")
        import traceback

        traceback.print_exc()
        raise


def bronze_to_silver():
    """Clean data from bronze bucket and save to silver bucket"""
    minio = get_minio_client()

    objects = list(minio.list_objects("bronze", prefix="pharmacy_sales_"))
    print(f"Found {len(objects)} files in bronze bucket")

    processed = 0
    for obj in objects:
        response = minio.get_object("bronze", obj.object_name)
        df = pd.read_parquet(io.BytesIO(response.read()))

        # Data cleaning
        if "sales" in df.columns:
            df = df[df["sales"] >= 0]

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        minio.put_object("silver", obj.object_name, buffer, len(buffer.getbuffer()))
        processed += 1
        print(f"✓ Processed {obj.object_name} ({len(df)} records)")

        response.close()
        response.release_conn()

    print(f"✓ Total files processed: {processed}")


def silver_to_gold():
    """Process silver data to create features and save to database"""
    from sqlalchemy import text

    minio = get_minio_client()
    engine = get_db_engine()

    try:
        objects = list(minio.list_objects("silver", prefix="pharmacy_sales_"))
        print(f"Found {len(objects)} files in silver bucket")

        if not objects:
            print("No data found in silver bucket.")
            return

        # Load all data
        data = []
        for obj in objects:
            response = minio.get_object("silver", object_name=obj.object_name)
            df_batch = pd.read_parquet(io.BytesIO(response.read()))
            data.append(df_batch)
            print(f"✓ Loaded {obj.object_name}: {len(df_batch)} records")
            response.close()
            response.release_conn()

        # Combine data
        print("Combining all data...")
        df = pd.concat(data, ignore_index=True)
        print(f"Total records: {len(df)}")
        del data

        # Sort for feature engineering
        df = df.sort_values(by=["distributor", "product_name", "city", "year", "month"])

        # Create features
        print("Creating feature aggregation...")
        feature = (
            df.groupby(
                [
                    "distributor",
                    "channel",
                    "sub_channel",
                    "city",
                    "product_name",
                    "product_class",
                    "sales_team",
                    "year",
                    "month",
                ]
            )
            .agg(
                total_quantity=("quantity", "sum"),
                total_sales=("sales", "sum"),
                avg_price=("price", "mean"),
            )
            .reset_index()
        )
        del df

        # Calculate rolling metrics
        print("Calculating rolling metrics...")
        feature["rolling_avg_3m_sales"] = feature.groupby(
            ["distributor", "product_name", "city"]
        )["total_sales"].transform(lambda x: x.rolling(window=3, min_periods=1).mean())

        feature["sales_growth_pct"] = feature.groupby(
            ["distributor", "product_name", "city"]
        )["total_sales"].transform(lambda x: x.pct_change() * 100)

        # Clean data
        feature = feature.replace([np.inf, -np.inf], np.nan)
        feature = feature.fillna(0)

        # Write to database
        print(f"Writing {len(feature)} records to database...")
        feature.to_sql(
            "sales_feature",
            engine,
            schema="features",
            if_exists="replace",
            index=False,
            chunksize=500,
            method="multi",
        )

        # Verify
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM features.sales_feature"))
            count = result.scalar()
            print(f"✓ Verified: {count} records in features.sales_feature")

    finally:
        engine.dispose()
        print("Database connection closed")


# Define DAG
with DAG(
    dag_id="sales_feature_pipeline_v2",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sales_pharmacy", "feature_engineering", "v2"],
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
) as dag:

    task_kafka_to_bronze = PythonOperator(
        task_id="kafka_to_bronze",
        python_callable=kafka_to_bronze,
    )

    task_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
    )

    task_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    task_kafka_to_bronze >> task_bronze_to_silver >> task_silver_to_gold

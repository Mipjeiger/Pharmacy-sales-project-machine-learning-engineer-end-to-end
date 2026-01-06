"""
Sales Feature Pipeline - Bronze -> Silver -> Gold
Kafka to MinIO to PostgreSQL data pipeline
"""

import io
import os
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_env_path():
    """Get project root and .env path"""
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return root, os.path.join(root, ".env")


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
    """Create PostgreSQL engine"""
    from sqlalchemy import create_engine
    from dotenv import load_dotenv

    _, env_path = get_env_path()
    load_dotenv(dotenv_path=env_path)

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Database environment variables are not fully set.")

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)


# Ensure minio bucket exist
def ensure_buckets_exists():
    """Ensure all required buckets exists"""
    minio = get_minio_client()
    required_buckets = ["bronze", "silver", "gold", "analytics"]

    for bucket in required_buckets:
        try:
            if not minio.bucket_exists(bucket):
                minio.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
            else:
                print(f"Bucket exists: {bucket}")
        except Exception as e:
            print(f"Error checking/creating bucket {bucket}: {e}")


def kafka_to_bronze():
    """Consume Kafka messages and save to MinIO bronze bucket"""
    import sys
    import importlib.util

    # Ensure buckets exist
    ensure_buckets_exists()

    project_root, _ = get_env_path()
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Load consumer module dynamically to avoid kafka package conflict
    consumer_path = os.path.join(project_root, "kafka", "consumer.py")

    if not os.path.exists(consumer_path):
        raise FileNotFoundError(f"Kafka consumer module not found at {consumer_path}")

    spec = importlib.util.spec_from_file_location("kafka_consumer", consumer_path)
    consumer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(consumer)

    minio = get_minio_client()
    count = 0

    print("Reading from Kafka...")
    try:
        for batch in consumer.read_batch(limit=1000, max_batches=5):
            if not batch:
                continue

            df = pd.DataFrame(batch)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            filename = f"pharmacy_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{count}.parquet"
            minio.put_object("bronze", filename, buffer, len(buffer.getbuffer()))
            print(f"✓ {filename}: {len(df)} records")
            count += 1

        print(f"✓ Created {count} files in bronze")

    except Exception as e:
        print(f"Error in kafka_to_bronze: {e}")
        raise


def bronze_to_silver():
    """Clean bronze data and save to silver bucket"""
    minio = get_minio_client()
    try:
        objects = list(minio.list_objects("bronze", prefix="pharmacy_sales_"))

        if not objects:
            print("No files found in bronze bucket.")
            return

        print(f"Processing {len(objects)} files...")
        for obj in objects:
            try:
                response = minio.get_object("bronze", obj.object_name)
                df = pd.read_parquet(io.BytesIO(response.read()))
                response.close()  # Close connection after reading
                response.release_conn()

                # Clean negative sales
                if "sales" in df.columns:
                    df = df[df["sales"] >= 0]

                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                minio.put_object(
                    "silver", obj.object_name, buffer, len(buffer.getbuffer())
                )
                print(f"✓ {obj.object_name}: {len(df)} records")

            except Exception as e:
                print(f"Error processing {obj.object_name}: {e}")
                raise
        print(f"✓ Processed {len(objects)} files to silver bucket.")
    except Exception as e:
        print(f"Error in bronze_to_silver: {e}")
        raise


def silver_to_gold():
    """Create features and save to PostgreSQL"""
    from sqlalchemy import text

    minio = get_minio_client()
    engine = get_db_engine()

    try:
        objects = list(minio.list_objects("silver", prefix="pharmacy_sales_"))
        if not objects:
            print("No data in silver bucket")
            return

        print(f"Loading {len(objects)} files...")

        # Load and combine data
        dfs = []
        for obj in objects:
            try:
                response = minio.get_object("silver", obj.object_name)
                dfs.append(pd.read_parquet(io.BytesIO(response.read())))
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"Error reading {obj.object_name}: {e}")
                raise

        df = pd.concat(dfs, ignore_index=True)
        print(f"Total: {len(df)} records")
        del dfs

        # Sort data
        df = df.sort_values(["distributor", "product_name", "city", "year", "month"])

        # Aggregate features
        print("Creating features...")
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
        del df  # delete dataframe to free memory

        # Add rolling metrics
        print("Adding rolling metrics...")
        grp = feature.groupby(["distributor", "product_name", "city"])
        feature["rolling_avg_3m_sales"] = grp["total_sales"].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        feature["sales_growth_pct"] = grp["total_sales"].transform(
            lambda x: x.pct_change() * 100
        )

        # Clean NaN and Inf
        feature = feature.replace([np.inf, -np.inf], np.nan).fillna(0)

        # Save to MinIO gold bucket
        print(f"Saving {len(feature)} records to gold bucket...")
        buffer = io.BytesIO()
        feature.to_parquet(buffer, index=False)
        buffer.seek(0)

        gold_filename = (
            f"sales_feature_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        minio.put_object("gold", gold_filename, buffer, len(buffer.getbuffer()))
        print(f"✓ Saved to gold/{gold_filename}")
        del buffer  # Free memory

        # Save analytics version (aggregated summary)
        print("Creating analytics summary...")
        analytics = (
            feature.groupby(["distributor", "city", "year", "month"])
            .agg(
                total_quantity=("total_quantity", "sum"),
                total_sales=("total_sales", "sum"),
                avg_price=("avg_price", "mean"),
                product_count=("product_name", "nunique"),
            )
            .reset_index()
        )

        buffer_analytics = io.BytesIO()
        analytics.to_parquet(buffer_analytics, index=False)
        buffer_analytics.seek(0)

        analytics_filename = (
            f"sales_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        minio.put_object(
            "analytics",
            analytics_filename,
            buffer_analytics,
            len(buffer_analytics.getbuffer()),
        )
        print(
            f"✓ Saved to analytics/{analytics_filename} ({len(analytics)} summary records)"
        )
        del buffer_analytics, analytics  # Free memory

        # Write to database in smaller batches
        print(f"Writing {len(feature)} records to database...")

        batch_size = 1000
        for i in range(0, len(feature), batch_size):
            batch = feature.iloc[i : i + batch_size]
            batch.to_sql(
                "sales_feature",
                engine,
                schema="features",
                if_exists="replace" if i == 0 else "append",
                index=False,
                method=None,
            )
            print(f"  ✓ Batch {i//batch_size + 1}: {len(batch)} records")
            del batch

        # Verify
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM features.sales_feature")
            ).scalar()
            print(f"✓ Verified: {count} records in database")

    finally:
        engine.dispose()


# DAG definition
with DAG(
    dag_id="sales_feature_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pharmacy", "etl"],
    default_args={"owner": "airflow", "retries": 2, "retry_delay": 60},
) as dag:

    t1 = PythonOperator(task_id="kafka_to_bronze", python_callable=kafka_to_bronze)
    t2 = PythonOperator(task_id="bronze_to_silver", python_callable=bronze_to_silver)
    t3 = PythonOperator(task_id="silver_to_gold", python_callable=silver_to_gold)

    t1 >> t2 >> t3

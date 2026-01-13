import duckdb
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import pandas as pd


# Load environment variables
load_dotenv()

# Postgresql connection from environment variables for credentials key
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

pg_engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

print(f"✓ PostgreSQL database: {db_name}")

# DuckDB (temporary for reading MinIO)
con = duckdb.connect()

# Install and load httpfs extension
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Set MinIO credentials
con.execute(
    """
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    """
)

print("✓ DuckDB MinIO configuration set")

# Main execution
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🦆 LOADING DATA FROM MINIO TO POSTGRESQL")
    print("=" * 60)

    # Step 1: Create schema in PostgreSQL
    print("\n[1/5] Creating schema 'silver' in PostgreSQL...")
    with pg_engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        conn.commit()
    print("✓ Schema 'silver' created")

    # Step 2: Get latest file from MinIO
    print("\n[2/5] Finding latest file in MinIO...")
    latest_file = con.execute(
        """
        SELECT file 
        FROM glob('s3://silver/pharmacy_sales_*.parquet')
        ORDER BY file DESC
        LIMIT 1
    """
    ).fetchone()

    if latest_file:
        print(f"✓ Latest file: {latest_file[0]}")
    else:
        print("⚠️  No files found in silver bucket")
        exit(1)

    # step 3: Read data from MinIO using DuckDB
    print("\n[3/5 reading data from MinIO using DuckDB...]")
    df = con.execute(
        f"""
                     SELECT * FROM read_parquet('{latest_file[0]}')
                     """
    ).fetchdf()
    print(f"✓ Data read into DataFrame with {len(df)} records from MinIO")

    # Step 4: Write to PostgreSQL
    print("\n[4/5] writing data into PostgreSQL...")

    # Write in batches to avoid memory issues
    batch_size = 1000
    total_batches = (len(df) + batch_size - 1) // batch_size

    for i in range(0, len(df), batch_size):
        batch_num = i // batch_size + 1
        batch = df.iloc[i : i + batch_size]

        batch.to_sql(
            "pharmacy_sales",
            pg_engine,
            schema="silver",
            if_exists="replace" if i == 0 else "append",
            index=False,
            method="multi",
            chunksize=500,
        )
        print(
            f"  ✓ Batch {batch_num}/{total_batches} inserted with {len(batch)} records"
        )
    print(
        f"✓ All {len(df)} records written to PostgreSQL table 'silver.pharmacy_sales'"
    )

    # Step 5: Validate data in posgreSQL
    print("\n[5/5] Validating data in PostgreSQL...")
    with pg_engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total_records,
                    MIN(year) AS min_year,
                    MAX(year) AS max_year,
                    SUM(sales) AS total_sales,
                    COUNT(DISTINCT distributor) AS unique_distributors
                FROM silver.pharmacy_sales
                 """
            )
        )
        stats = result.fetchone()

        # Show data sample
        print("\nData Sample from postgreSQL:")
        sample = pd.read_sql("SELECT * FROM silver.pharmacy_sales LIMIT 20;", pg_engine)
        print(sample.to_string(index=False))

        # Close connections
        con.close()
        pg_engine.dispose()

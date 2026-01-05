import json
import psycopg2
import os
from datetime import datetime, date
from dotenv import load_dotenv
from kafka import KafkaProducer
from sqlalchemy import create_engine, text

# Load environment variables from .env file
# Get the absolute path to the project root (parent of kafka folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
env_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path=env_path)

# Retrieve database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# Helper function to handle datetime to JSON serialization
def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# Initialize database connection
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

# initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=json_serial).encode("utf-8"),
)


# Fetch data from the database and send to Kafka topic
def send_data_to_kafka():
    try:
        with engine.connect() as connection:
            query = text("SELECT * FROM raw.pharmacy_sales;")
            result = connection.execute(query)

            # Getting column names from the result
            cols = result.keys()
            rows = result.fetchall()

            if not rows:
                print("No data found in the database.")
                return

            for row in rows:
                # row is a SQLAlchemy Row object, convert it to a dictionary
                data = dict(zip(cols, row))
                producer.send("pharmacy_sales", value=data)
                print(f"Sent data to Kafka: {data}")

            producer.flush()  # Ensure all messages are sent
            print(
                f"Successfully sent {len(rows)} records to Kafka topic 'pharmacy_sales'."
            )
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    print("Starting to send data to Kafka...")
    send_data_to_kafka()

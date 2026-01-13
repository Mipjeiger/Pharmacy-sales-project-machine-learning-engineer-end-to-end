from feast import Entity, FeatureView, Field
from feast.types import String, Float64, Int64, UnixTimestamp  # For field SCHEMA
from feast.value_type import ValueType  # For entity value types
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from datetime import timedelta

# ============================================
# Entities (Identifiers for distributed features)
# ============================================

# Distributor Entity
distributor = Entity(
    name="distributor",
    join_keys=["distributor"],
    value_type=ValueType.STRING,
    description="Pharmaceutical distributor",
)

# City Entity
city = Entity(
    name="city",
    join_keys=["city"],
    value_type=ValueType.STRING,
    description="City where the pharmacy distributor is located",
)

# Product Entity
product = Entity(
    name="product_name",
    join_keys=["product_name"],
    value_type=ValueType.STRING,
    description="Name of the pharmaceutical product",
)

# ============================================
# Data Source
# ============================================

# PostgreSQL source - Silver Layer
pharmacy_sales_source = PostgreSQLSource(
    name="pharmacy_sales_source",
    query="""
        SELECT
            distributor,
            city,
            country,
            channel,
            sub_channel,
            product_name,
            product_class,
            quantity,
            price,
            sales_team,
            month,
            year,
            quantity as total_quantity,
            sales AS total_sales,
            price as avg_price,
            NOW() as event_timestamp
        FROM silver.pharmacy_sales
            """,
    timestamp_field="event_timestamp",
    description="Pharmacy sales data from PostgreSQL silver SCHEMA which is ingested and modified to fit feature store needs",
)

# ============================================
# Feature Views
# ============================================

# Sales Features
sales_feature_view = FeatureView(
    name="sales_features",
    entities=[distributor, city, product],
    schema=[
        Field(name="country", dtype=String),
        Field(name="product_class", dtype=String),
        Field(name="channel", dtype=String),
        Field(name="sub_channel", dtype=String),
        Field(name="year", dtype=Int64),
        Field(name="month", dtype=String),
        Field(name="total_quantity", dtype=Int64),
        Field(name="total_sales", dtype=Float64),
        Field(name="avg_price", dtype=Float64),
        Field(name="sales_team", dtype=String),
    ],
    source=pharmacy_sales_source,
    ttl=timedelta(days=365),
    online=True,
    tags={"team": "Machine_Learning_Engineer", "project": "pharmacy_sales"},
)

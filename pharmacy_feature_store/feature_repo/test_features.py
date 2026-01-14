import os
from feast import FeatureStore
import pandas as pd
from datetime import datetime

# Verify we're in correct directory
print(f"Current directory: {os.getcwd()}")
print(f"Files in directory: {os.listdir('.')}")

# Initialize the Feature Store
try:
    store = FeatureStore(repo_path=".")
    print("Feature Store loaded successfully.")
except Exception as e:
    print(f"Error loading Feature Store: {e}")
    exit(1)

print("✓ Feature Store initialized")
print("=" * 60)
print(f"Project: {store.project}")
print(f"Registry: {store.config.registry}")

# List entities and feature views
try:
    entities = store.list_entities()
    print(f"\n ({len(entities)}) Entities:")
    for e in entities:
        print(f" - {e.name} (value_type: {e.value_type})")
except Exception as e:
    print(f"Error listing entities: {e}")

# List feature views
try:
    feature_views = store.list_feature_views()
    print(f"\n ({len(feature_views)}) Feature Views:")
    for fv in feature_views:
        print(f" - {fv.name}")
        print(f"   Entities: {[entity.name for entity in fv.entities]}")
        print(f"   Features: {[field.name for field in fv.schema]}")
except Exception as e:
    print(f"Error listing feature views: {e}")

# Check if sales_features exists
print("\n" + "=" * 60)
print("Checking sales_features existence...")
print("=" * 60)

try:
    fv = store.get_feature_view("sales_features")
    print(f"✓ Feature View 'sales_features' found with {len(fv.schema)} features.")
    print(f"Entities: {[entity.name for entity in fv.entities]}")
    print(f"Features: {[field.name for field in fv.schema]}")
except Exception as e:
    print(f"Error retrieving feature view 'sales_features': {e}")
    exit(1)

print("=" * 60)
print("Testing Historical feature retrieval...")
print("=" * 60)

# Get online features for a list of entities
entity_df = pd.DataFrame(
    {
        "distributor": ["Beier", "Beier"],
        "city": ["Lublin", "Bielsko-Biała"],
        "product_name": ["Kinenadryl", "Abobozolid"],
        "event_timestamp": [datetime.now(), datetime.now()],
    }
)

print("Entity DataFrame:")
print(entity_df)

try:
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "sales_features:total_quantity",
            "sales_features:total_sales",
            "sales_features:avg_price",
            "sales_features:year",
            "sales_features:month",
            "sales_features:product_class",
            "sales_features:sales_team",
        ],
    ).to_df()

    print("\nRetrieved Historical Features:")
    print(training_df)
    print(f"\nShape: {training_df.shape}")
    print(f"\nColumns: {training_df.columns.tolist()}")

except Exception as e:
    print(f"Error retrieving historical features: {e}")
    print(f" {type(e).__name__}: {e}")
    import traceback

    traceback.print_exc()

print("\n✓ Historical feature retrieval test completed")

from feast import FeatureStore
import pandas as pd
from datetime import datetime

# Initialize the Feature Store
store = FeatureStore(repo_path=".")

# Get online features for a list of entities
entity_rows = [{"distributor": ""}]

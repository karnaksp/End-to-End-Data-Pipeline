def store_features(feature_data):
    """
    Store feature data into a hypothetical Feature Store (e.g., Feast).
    In practice, you would:
      1. Connect to your Feast deployment
      2. Create or update the feature table
      3. Ingest data (batch or streaming)
      4. Version the features for consistent retrieval
    """
    print("Storing features in Feature Store (stub) with data:")
    for record in feature_data:
        print(f" - {record}")

if __name__ == "__main__":
    # Example usage
    sample_features = [
        {"device_id": 1001, "avg_reading": 65.4, "max_reading": 80.0},
        {"device_id": 1002, "avg_reading": 70.2, "max_reading": 78.5},
    ]
    store_features(sample_features)

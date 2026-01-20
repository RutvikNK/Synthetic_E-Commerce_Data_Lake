import base64
import json
import os
from datetime import datetime
from google.cloud import storage
import functions_framework

storage_client = storage.Client()

# Simpler Config: Just one main bucket
BUCKET_NAME = os.environ.get("BUCKET_NAME")
QUARANTINE_BUCKET = os.environ.get("QUARANTINE_BUCKET")

@functions_framework.cloud_event
def ingest_event(cloud_event):
    try:
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        event_data = json.loads(pubsub_message)
        
        event_type = event_data.get("event_type")
        timestamp_str = event_data.get("timestamp")
        event_id = event_data.get("event_id", "unknown")

        if not timestamp_str or not event_type:
            raise ValueError("Missing 'timestamp' or 'event_type'")

        # Parse Time
        dt = datetime.fromisoformat(timestamp_str)
        
        # Construct Hive-Style Path
        # gs://bucket/event_type=purchase/year=2026/month=01/day=19/file.json
        partition_path = f"event_type={event_type}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        blob_name = f"{partition_path}/{event_id}.json"
        
        # 3. Save
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(event_data), content_type="application/json")
        
        print(f"✅ Saved to {BUCKET_NAME}/{blob_name}")

    except Exception as e:
        print(f"❌ Error: {e}")
        if QUARANTINE_BUCKET:
            bucket = storage_client.bucket(QUARANTINE_BUCKET)
            now = datetime.now()
            q_path = f"failed/year={now.year}/month={now.month:02d}/day={now.day:02d}"
            blob = bucket.blob(f"{q_path}/{datetime.now().timestamp()}_error.json")
            blob.upload_from_string(json.dumps({"error": str(e), "payload": str(pubsub_message)}))
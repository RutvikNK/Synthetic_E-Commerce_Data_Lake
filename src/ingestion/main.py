import base64
import json
import os
from datetime import datetime
from google.cloud import storage
import functions_framework

# Initialize Storage Client once (global scope)
storage_client = storage.Client()

# Load Bucket Names from Env Vars
BUCKET_MAP = {
    "ad_click": os.environ.get("BUCKET_AD_CLICK"),
    "page_view": os.environ.get("BUCKET_PAGE_VIEW"),
    "add_to_cart": os.environ.get("BUCKET_ADD_TO_CART"),
    "purchase": os.environ.get("BUCKET_PURCHASE")
}
QUARANTINE_BUCKET = os.environ.get("QUARANTINE_BUCKET")

@functions_framework.cloud_event
def ingest_event(cloud_event):
    """
    Triggered by a Pub/Sub message.
    Routes data to specific buckets based on event_type.
    """
    try:
        # Decode Message
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        event_data = json.loads(pubsub_message)
        
        # Extract Event Type & Timestamp
        event_type = event_data.get("event_type")
        timestamp_str = event_data.get("timestamp")
        event_id = event_data.get("event_id", "unknown_id")

        # Validation: Check for Poison Pill (Missing Timestamp)
        if not timestamp_str:
            raise ValueError("Missing 'timestamp' field")

        # Route to Correct Bucket
        target_bucket_name = BUCKET_MAP.get(event_type)
        
        if not target_bucket_name:
            # If event_type is unknown or None, treat as malformed
            raise ValueError(f"Unknown or missing event_type: {event_type}")

        # Determine Partition Path (year=YYYY/month=MM/day=DD)
        dt = datetime.fromisoformat(timestamp_str)
        partition_path = f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        
        # Save to GCS
        bucket = storage_client.bucket(target_bucket_name)
        blob_name = f"{partition_path}/{event_id}.json"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(event_data), content_type="application/json")
        
        print(f"✅ Saved {event_type} to {target_bucket_name}/{blob_name}")

    except Exception as e:
        print(f"❌ Error processing event: {e}")
        # Send to Quarantine Bucket
        if QUARANTINE_BUCKET:
            bucket = storage_client.bucket(QUARANTINE_BUCKET)
            # Use current time for quarantine partitioning
            now = datetime.utcnow()
            q_path = f"failed/year={now.year}/month={now.month:02d}/day={now.day:02d}"
            # Save the raw message (or error details)
            blob = bucket.blob(f"{q_path}/{datetime.utcnow().timestamp()}_error.json")
            error_payload = {
                "error": str(e),
                "original_payload": pubsub_message if 'pubsub_message' in locals() else "parse_error"
            }
            blob.upload_from_string(json.dumps(error_payload), content_type="application/json")
            print(f"⚠️ Sent to quarantine: {QUARANTINE_BUCKET}")
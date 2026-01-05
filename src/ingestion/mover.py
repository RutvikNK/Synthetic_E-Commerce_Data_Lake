import base64
import json
import os
from datetime import datetime
from google.cloud import storage
import functions_framework

# Initialize the Google Cloud Storage client
storage_client = storage.Client()

# Get Bucket Name from Environment Variable
BUCKET_NAME = os.environ.get("BUCKET_NAME")

@functions_framework.cloud_event
def ingest_event(cloud_event):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    1. Decodes the message.
    2. Parses the timestamp.
    3. Saves to GCS in a partitioned folder structure.
    """
    try:
        # Pub/Sub sends data wrapped in base64
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        event_data = json.loads(pubsub_message)
        
        # Use the event's timestamp if it exists, otherwise use 'now'
        timestamp = event_data.get("timestamp")
        if timestamp:
            dt = datetime.fromtimestamp(timestamp)
        else:
            dt = datetime.utcnow()

        # Create the Hive-style partition path
        # Example: "year=2024/month=01/day=15"
        partition_path = (
            f"year={dt.year}/"
            f"month={dt.month:02d}/"
            f"day={dt.day:02d}"
        )

        # Save the event data to GCS
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Filename: events/{partition}/event_{uuid}.json
        file_name = f"events/{partition_path}/event_{event_data.get('event_id')}.json"
        
        blob = bucket.blob(file_name)
        blob.upload_from_string(
            data=json.dumps(event_data),
            content_type="application/json"
        )
        
        print(f"✅ Saved {file_name}")

    except Exception as e:
        print(f"❌ Error processing event: {e}")
        # Re-raising the error tells Pub/Sub to "Retry" sending this message later
        raise e
import pytest
import json
import base64
from unittest.mock import MagicMock, patch

from src.ingestion.main import ingest_event

# Helper to create a fake CloudEvent
class FakeCloudEvent:
    def __init__(self, data):
        self.data = data

class TestIngestEvent:
    @patch("src.ingestion.main.storage_client")
    def test_ingest_event_success(self, mock_client):
        """
        Test that the function:
        1. Decodes the Pub/Sub message
        2. Generates the correct path (year=.../month=...)
        3. Calls 'upload_from_string' on the bucket
        """
        # 1. Prepare Fake Data
        fake_data = {
            "event_id": "test-123",
            "timestamp": 1715000000, # Fixed timestamp (May 2024)
            "event_type": "purchase"
        }
        encoded_data = base64.b64encode(json.dumps(fake_data).encode("utf-8"))
        
        # Structure it like a real Pub/Sub event
        cloud_event = FakeCloudEvent(
            data={"message": {"data": encoded_data}}
        )

        # Create a fake bucket and fake blob so the code doesn't crash
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        ingest_event(cloud_event)
        
        mock_client.bucket.assert_called()
        
        # 1715000000 is Year 2024, Month 05, Day 06
        expected_filename = "events/year=2024/month=05/day=06/event_test-123.json"
        mock_bucket.blob.assert_called_with(expected_filename)
        
        mock_blob.upload_from_string.assert_called_once()
        
        uploaded_data = json.loads(mock_blob.upload_from_string.call_args[1]['data'])
        assert uploaded_data["event_id"] == "test-123"
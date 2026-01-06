import pytest
from unittest.mock import MagicMock, patch
from src.generator.producer import DataProducer

class TestDataProducer:
    @pytest.fixture
    def mock_publisher(self):
        """
        Patches the PublisherClient so we can create DataProducer 
        without needing real GCP credentials.
        """
        with patch("src.generator.producer.pubsub_v1.PublisherClient") as mock_client:
            yield mock_client

    def test_generate_event_structure(self, mock_publisher):
        """Ensure the event has all required keys."""
        producer = DataProducer(project_id="test", topic_id="test")
        
        event = producer.generate_event()
        
        required_keys = ["event_id", "user_id", "timestamp", "event_type"]
        for key in required_keys:
            assert key in event

    def test_purchase_has_revenue(self, mock_publisher):
        """Ensure purchase events always have positive revenue."""
        producer = DataProducer(project_id="test", topic_id="test")
        
        # Retry loop to ensure checking a purchase event
        for _ in range(100):
            event = producer.generate_event()
            if event["event_type"] == "purchase":
                assert "revenue" in event
                assert event["revenue"] > 0
                assert "transaction_id" in event
                return

    def test_publish_data_calls_gcp(self, mock_publisher):
        """
        Verify that publish_data() actually calls the GCP library
        with the correct arguments.
        """
        mock_instance = mock_publisher.return_value
        expected_topic = "projects/my-project/topics/my-topic"
        mock_instance.topic_path.return_value = expected_topic
        
        producer = DataProducer(project_id="my-project", topic_id="my-topic")
        
        mock_instance = producer.publisher 
        
        fake_data = {"key": "value"}
        producer.publish_data(fake_data)
        
        mock_instance.publish.assert_called_once()
        
        # args[0] is the first argument passed to publish()
        args, _ = mock_instance.publish.call_args
        assert "projects/my-project/topics/my-topic" in args[0]

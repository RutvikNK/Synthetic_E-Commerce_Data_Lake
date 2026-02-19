import pytest
from unittest.mock import MagicMock, patch, ANY
from src.generator.producer import DataProducer, PRODUCTS

@pytest.fixture
def mock_publisher_client():
    """Mocks the Google Pub/Sub Client to avoid real network calls."""
    with patch("src.generator.producer.pubsub_v1.PublisherClient") as mock:
        yield mock

@pytest.fixture
def producer(mock_publisher_client):
    """Creates an instance of DataProducer with a mocked publisher."""
    return DataProducer(project_id="test-project", topic_id="test-topic")

def test_init_sets_topic_path(producer, mock_publisher_client):
    """Verify the topic path is constructed correctly on initialization."""
    # The mock client instance is the return value of the class call
    mock_instance = mock_publisher_client.return_value
    
    # Check that topic_path was called with the correct project and topic
    mock_instance.topic_path.assert_called_with("test-project", "test-topic")

def test_generate_event_structure(producer):
    """Ensure a single event has all required fields."""
    # Use the first product from your global list for consistency
    test_product = PRODUCTS[0]
    
    event = producer.generate_event("user_123", "session_456", "page_view", product=test_product)
    
    assert event["user_id"] == "user_123"
    assert event["session_id"] == "session_456"
    assert event["event_type"] == "page_view"
    assert "timestamp" in event
    assert "event_id" in event
    # Check product fields were merged correctly
    assert event["product_id"] == test_product["id"]
    assert event["price"] == test_product["price"]

def test_simulate_user_journey_purchase_logic(producer):
    """
    Verify the 'Funnel' logic: A purchase MUST be preceded by view and cart.
    """
    # Force the random choice to be "purchase"
    with patch("random.choices", return_value=["purchase"]):
        # Force "random.choice" to be False so we skip the Ad Click check for this specific test
        # (Or we can just filter the results)
        # Also force number of sessions to 1
        with patch("random.randint", return_value=1):
            journey = producer.simulate_user_journey()
        
    event_types = [e["event_type"] for e in journey]
    
    # 1. Check logical order presence
    assert "page_view" in event_types
    assert "add_to_cart" in event_types
    assert "purchase" in event_types
    
    # 2. Check consistency (same user, same product)
    user_ids = set(e["user_id"] for e in journey)
    session_ids = set(e["session_id"] for e in journey)
    product_ids = set(e.get("product_id") for e in journey if "product_id" in e)
    
    assert len(user_ids) == 1, "Journey should belong to one unique user"
    assert len(session_ids) == 1, "Journey should belong to one unique session for this test"
    assert len(product_ids) == 1, "Journey should focus on one specific product"

def test_ad_click_journey(producer):
    """Verify ad_click journeys contain ad metadata."""
    with patch("random.choices", return_value=["ad_click_only"]):
        with patch("random.choice", return_value=True): # Force ad path
            journey = producer.simulate_user_journey()
            
    assert len(journey) >= 1
    first_event = journey[0]
    assert first_event["event_type"] == "ad_click"
    assert "ad_source" in first_event
    assert "campaign_id" in first_event

def test_poison_pill_injection(producer):
    """Verify that the poison pill logic actually deletes the timestamp."""
    # Force random() to return a number < 0.01 (e.g., 0.001)
    with patch("random.random", return_value=0.001):
        event = producer.generate_event("bad_user", "bad_session", "page_view")
        assert "timestamp" not in event, "Poison pill should remove timestamp"

def test_publish_data(producer):
    """Verify that the publisher client is actually called."""
    mock_publisher = producer.publisher  # This is the mock created in __init__
    test_data = {"test": "data"}
    
    producer.publish_data(test_data)
    
    # Ensure publish was called
    mock_publisher.publish.assert_called_once()
    
    # Check arguments (topic path and data bytes)
    call_args = mock_publisher.publish.call_args
    assert call_args[0][0] == producer.topic_path
    assert b'{"test": "data"}' in call_args[1]['data']
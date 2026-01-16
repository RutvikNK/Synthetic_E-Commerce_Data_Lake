import json
import time
import random
import os
import uuid

from datetime import datetime

from faker import Faker
from google.cloud import pubsub_v1
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "")

PRODUCTS = [
    {"id": "prod_001", "name": "Wireless Noise-Canceling Headphones", "category": "Audio", "price": 299.99},
    {"id": "prod_002", "name": "Ergonomic Mechanical Keyboard", "category": "Accessories", "price": 149.50},
    {"id": "prod_003", "name": "4K Ultra HD Gaming Monitor", "category": "Electronics", "price": 499.00},
    {"id": "prod_004", "name": "Smart Home Security Camera", "category": "Smart Home", "price": 89.99},
    {"id": "prod_005", "name": "Portable Espresso Maker", "category": "Kitchen", "price": 65.00},
    {"id": "prod_006", "name": "Waterproof Hiking Backpack", "category": "Outdoors", "price": 110.00},
]

class DataProducer:
    def __init__(self, project_id: str, topic_id: str):
        self.project_id: str = project_id
        self.topic_id: str = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.__faker = Faker()

    def generate_event(self, user_id, event_type, product=None, referrer=None):
        """Creates a single event payload."""
        event_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "user_id": user_id,
            "timestamp": timestamp,
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "location": random.choice(["US", "UK", "DE", "FR", "JP", "CA"]),
        }

        # Add product details if applicable
        if product:
            payload.update({
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"]
            })
        
        # Add ad-specific details
        if event_type == "ad_click":
            payload["ad_source"] = random.choice(["google", "facebook", "tiktok", "email_blast"])
            payload["campaign_id"] = f"camp_{random.randint(100, 999)}"

        # Simulate Chaos (Poison Pill) - 1% chance to break schema
        if random.random() < 0.01:
            print(f"😈 Injecting POISON PILL event: {event_id}")
            # Intentionally removing timestamp to trigger DLQ
            del payload["timestamp"]

        return payload

    def simulate_user_journey(self):
        """
        Generates a logical sequence of events for a single user session.
        """
        user_id = str(uuid.uuid4())
        selected_product = random.choice(PRODUCTS)
        
        # Weights define how likely a user is to convert.
        # 0: Ad Click Only (Bounce)
        # 1: View Only (Window Shopper)
        # 2: View -> Cart (Abandoner)
        # 3: View -> Cart -> Purchase (Buyer)
        journey_type = random.choices(
            ["ad_click_only", "browse", "cart_abandon", "purchase"], 
            weights=[10, 40, 30, 20]  # 20% conversion rate (optimistic!)
        )[0]

        events_to_publish = []

        # Step 1: Did they come from an Ad? (50% chance)
        if random.choice([True, False]) or journey_type == "ad_click_only":
            events_to_publish.append(self.generate_event(user_id, "ad_click"))
            if journey_type == "ad_click_only":
                return events_to_publish

        # Step 2: The Funnel Logic
        if journey_type in ["browse", "cart_abandon", "purchase"]:
            # They MUST view the product first
            events_to_publish.append(self.generate_event(user_id, "page_view", selected_product))

        if journey_type in ["cart_abandon", "purchase"]:
            # They MUST add to cart before purchasing
            events_to_publish.append(self.generate_event(user_id, "add_to_cart", selected_product))
        
        if journey_type == "purchase":
            # Finally, the purchase
            events_to_publish.append(self.generate_event(user_id, "purchase", selected_product))
            
        return events_to_publish

    def publish_data(self, data):
        data_str = json.dumps(data)
        data_bytes = data_str.encode("utf-8")
        future = self.publisher.publish(self.topic_path, data=data_bytes)
        print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    producer = DataProducer(PROJECT_ID, TOPIC_ID)

    print(f"🚀 Starting Producer... Sending events to {TOPIC_ID}")
    try:
        while True:
            # Generate a consistent journey for one user
            journey = producer.simulate_user_journey()
            
            for event in journey:
                producer.publish_data(event)
                # Small delay between user actions (reading page, clicking button)
                time.sleep(random.uniform(0.5, 2.0))
            
            # Wait before the next random user arrives
            print("--- New User Session ---")
            time.sleep(random.uniform(1.0, 3.0))
            
    except KeyboardInterrupt:
        print("🛑 Producer stopped.")
    except Exception as e:
        print(f"\n⚠️ Error: {e}")
        print("Did you create the Pub/Sub topic and authenticate?")
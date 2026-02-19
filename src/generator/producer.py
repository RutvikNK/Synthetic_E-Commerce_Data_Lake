import datetime as dt
import json
import time
import random
import os
import uuid
import datetime as dt

from datetime import datetime
from typing import Any, List, Tuple

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

    def generate_event(self, user_id, session_id, event_type, product=None, referrer=None, timestamp_override=None):
        """Creates a single event payload."""
        event_id = str(uuid.uuid4())
        timestamp = timestamp_override or datetime.now().isoformat()
        
        payload = {
            "event_id": event_id,
            "session_id": session_id,
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
            payload.pop("timestamp", None)

        return payload

    def simulate_user_journey(self):
        """
        Generates a logical sequence of events for a user across multiple sessions.
        """
        import datetime as dt
        user_id = str(uuid.uuid4())
        events_to_publish = []
        
        # Determine number of sessions for this user (1 to 3)
        num_sessions = random.randint(1, 3)
        # Start time in the past to allow for realistic multi-day journeys
        current_time = datetime.now() - dt.timedelta(days=random.uniform(5, 10))
        
        for session_idx in range(num_sessions):
            session_id = str(uuid.uuid4())
            selected_product = random.choice(PRODUCTS)
            
            # Is this session broken by an "overnight" gap?
            has_overnight_gap = random.random() < 0.2
            
            journey_type = random.choices(
                ["ad_click_only", "browse", "cart_abandon", "purchase"], 
                weights=[10, 40, 30, 20]  # 20% conversion rate
            )[0]
            
            events: List[Tuple[str, Any]] = []
            
            if random.choice([True, False]) or journey_type == "ad_click_only":
                events.append(("ad_click", None))
            
            if journey_type in ["browse", "cart_abandon", "purchase"]:
                # Multiple page views
                num_views = random.randint(1, 4)
                for _ in range(num_views):
                    events.append(("page_view", selected_product))
                    
            if journey_type in ["cart_abandon", "purchase"]:
                # Multiple add_to_cart events possible
                num_add = random.randint(1, 2)
                for _ in range(num_add):
                    events.append(("add_to_cart", selected_product))
                
            if journey_type == "purchase":
                events.append(("purchase", selected_product))
                
            # Randomly pick where the overnight gap happens (if applicable and enough events exist)
            gap_event_index = random.randint(1, max(1, len(events) - 1)) if has_overnight_gap and len(events) > 1 else -1

            for i, (ev_type, prod) in enumerate(events):
                if has_overnight_gap and i == gap_event_index:
                    # add 8-12 hours gap (left page open overnight)
                    current_time += dt.timedelta(hours=random.uniform(8, 12))
                else:
                    # normal gap between clicks: 10 seconds to 5 mins
                    current_time += dt.timedelta(seconds=random.uniform(10, 300))
                    
                timestamp_str = current_time.isoformat()
                event_payload = self.generate_event(user_id, session_id, ev_type, prod, timestamp_override=timestamp_str)
                events_to_publish.append(event_payload)
            
            # Gap between sessions: 1 to 3 days
            current_time += dt.timedelta(days=random.uniform(1, 3))
            
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
            # Generate a consistent journey history for one user
            journey = producer.simulate_user_journey()
            
            for event in journey:
                producer.publish_data(event)
                # Small delay between publishing messages (not user wait time, just pacing)
                time.sleep(random.uniform(0.01, 0.1))
            
            # Wait before the next random user arrives
            print("--- New User History Published ---")
            time.sleep(random.uniform(1.0, 3.0))
            
    except KeyboardInterrupt:
        print("🛑 Producer stopped.")
    except Exception as e:
        print(f"\n⚠️ Error: {e}")
        print("Did you create the Pub/Sub topic and authenticate?")
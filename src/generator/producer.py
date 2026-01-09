import json
import time
import random
import os
from faker import Faker
from google.cloud import pubsub_v1

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "")

class DataProducer:
    def __init__(self, project_id: str, topic_id: str):
        self.project_id: str = project_id
        self.topic_id: str = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.__faker = Faker()

    def generate_event(self):
        """Creates a single synthetic e-commerce event."""
        # Introduce poison data with a small probability
        if random.random() < 0.01:
            print("😈 Generating POISON data!")
            return {
                "event_id": self.__faker.uuid4(),
                # Missing 'timestamp' will cause datetime.fromtimestamp to fail
                # Missing 'user_id' might break BigQuery schema
                "event_type": "poison_pill", 
                "bad_field": "This will crash the consumer" 
            }
        
        # Randomly decide what kind of event this is
        event_type = random.choice(['view_product', 'add_to_cart', 'click_ad', 'purchase'])
        
        # Build the event payload
        event_data = {
            "event_id": self.__faker.uuid4(),
            "user_id": self.__faker.random_int(min=1000, max=100000),
            "timestamp": time.time(),
            "event_type": event_type,
            "product_id": self.__faker.ean(length=13),
            "ip_address": self.__faker.ipv4(),
            "city": self.__faker.city(),
            "country": self.__faker.country(),
            "device": random.choice(['mobile', 'desktop', 'tablet']),
            "browser": self.__faker.user_agent()
        }

        # Add context-specific fields
        if event_type == 'purchase':
            # Purchases have revenue and transaction IDs
            event_data["revenue"] = round(random.uniform(10.0, 500.0), 2)
            event_data["transaction_id"] = self.__faker.uuid4()
        
        return event_data

    def publish_data(self, data):
        data_str = json.dumps(data)
        data_bytes = data_str.encode("utf-8")
        future = self.publisher.publish(self.topic_path, data=data_bytes)
        print(f"Published message ID: {future.result()}")

    def run(self, interval=5):
        while True:
            data = self.generate_event()
            self.publish_data(data)
            time.sleep(interval)

if __name__ == "__main__":
    producer = DataProducer(PROJECT_ID, TOPIC_ID)

    print(f"🚀 Starting generator streaming to: {producer.topic_path}")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            # 1. Generate a dictionary of fake data
            data = producer.generate_event()
            
            # 2. Publish the data to Pub/Sub
            producer.publish_data(data)
            
            # 5. Sleep to simulate a steady stream (10 events/sec)
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("\n🛑 Generator stopped.")
    except Exception as e:
        print(f"\n⚠️ Error: {e}")
        print("Did you create the Pub/Sub topic and authenticate?")
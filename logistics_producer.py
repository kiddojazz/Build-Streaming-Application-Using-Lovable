import json
import logging.config
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List

from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer
from pydantic import BaseModel, Field
from pythonjsonlogger import jsonlogger

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

class GracefulShutdown:
    """Class to handle graceful shutdown of the producer"""
    
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}. Starting graceful shutdown...")
        self.shutdown = True
    
    @property
    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested"""
        return self.shutdown

class LogisticsEvent(BaseModel):
    """Pydantic model for logistics event data validation"""
    event_id: str = Field(..., description="Unique identifier for the logistics event")
    timestamp: datetime = Field(..., description="Event timestamp")
    event_type: str = Field(..., description="Type of logistics event")
    shipment_id: str = Field(..., description="Unique identifier for the shipment")
    tracking_number: str = Field(..., description="Tracking number for the shipment")
    vehicle_id: str = Field(..., description="Vehicle identifier")
    driver_id: str = Field(..., description="Driver identifier")
    origin: Dict[str, str] = Field(..., description="Origin location details")
    destination: Dict[str, str] = Field(..., description="Destination location details")
    status: str = Field(..., description="Current status of the shipment")
    estimated_delivery: datetime = Field(..., description="Estimated delivery time")
    actual_delivery: datetime | None = Field(None, description="Actual delivery time")
    package_details: Dict = Field(..., description="Package information")
    weather_conditions: str = Field(..., description="Current weather conditions")
    delay_minutes: int = Field(0, description="Delay in minutes if any")

class LogisticsDataGenerator:
    """Class to generate fake logistics data"""
    
    def __init__(self):
        self.fake = Faker()
        self.event_types = ["PICKUP", "IN_TRANSIT", "DELIVERED", "DELAYED", "OUT_FOR_DELIVERY"]
        self.status_types = ["ON_TIME", "DELAYED", "COMPLETED", "CANCELLED"]
        self.weather_conditions = ["Clear", "Rainy", "Cloudy", "Snowy", "Windy"]
        self.vehicle_types = ["Truck", "Van", "Motorcycle"]
        
    def _generate_location(self) -> Dict[str, str]:
        """Generate a random location with address details"""
        return {
            "address": self.fake.street_address(),
            "city": self.fake.city(),
            "state": self.fake.state(),
            "postal_code": self.fake.zipcode(),
            "country": self.fake.country()
        }
    
    def _generate_package_details(self) -> Dict:
        """Generate random package details"""
        return {
            "weight": round(random.uniform(0.1, 100.0), 2),
            "dimensions": {
                "length": random.randint(1, 100),
                "width": random.randint(1, 100),
                "height": random.randint(1, 100)
            },
            "category": random.choice(["Electronics", "Clothing", "Food", "Furniture", "Documents"]),
            "handling_instructions": random.choice(["Fragile", "Handle with care", "Keep dry", "This side up"]),
            "vehicle_type": random.choice(self.vehicle_types)
        }
    
    def generate_event(self) -> LogisticsEvent:
        """Generate a single logistics event"""
        event_type = random.choice(self.event_types)
        base_timestamp = datetime.now()
        
        estimated_delivery = base_timestamp + timedelta(days=random.randint(1, 5))
        actual_delivery = None
        if event_type == "DELIVERED":
            actual_delivery = estimated_delivery + timedelta(minutes=random.randint(-120, 120))
        
        return LogisticsEvent(
            event_id=self.fake.uuid4(),
            timestamp=base_timestamp,
            event_type=event_type,
            shipment_id=self.fake.uuid4(),
            tracking_number=self.fake.bothify(text='EG##-????-####'),
            vehicle_id=self.fake.bothify(text='VH-####'),
            driver_id=self.fake.bothify(text='DR####'),
            origin=self._generate_location(),
            destination=self._generate_location(),
            status=random.choice(self.status_types),
            estimated_delivery=estimated_delivery,
            actual_delivery=actual_delivery,
            package_details=self._generate_package_details(),
            weather_conditions=random.choice(self.weather_conditions),
            delay_minutes=random.randint(0, 120) if random.random() < 0.3 else 0
        )

class KafkaLogisticsProducer:
    """Class to handle Kafka production of logistics events"""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.topic = os.getenv("KAFKA_TOPIC")
        self.batch_size = int(os.getenv("BATCH_SIZE", "100"))
        self.production_interval = int(os.getenv("PRODUCTION_INTERVAL", "2"))
        self.shutdown_handler = GracefulShutdown()
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )
        
        self.data_generator = LogisticsDataGenerator()
        logger.info(f"Initialized Kafka producer for topic: {self.topic}")
    
    def produce_events(self) -> None:
        """Continuously produce logistics events to Kafka"""
        events_produced = 0
        
        try:
            while not self.shutdown_handler.is_shutdown_requested:
                events = [
                    self.data_generator.generate_event().model_dump()  # Fixed Pydantic deprecation
                    for _ in range(self.batch_size)
                ]
                
                for event in events:
                    self.producer.send(self.topic, value=event)
                
                self.producer.flush()
                events_produced += len(events)
                logger.info(f"Produced {self.batch_size} events to topic {self.topic}")
                time.sleep(self.production_interval)
                
        except Exception as e:
            logger.error(f"Error producing events: {str(e)}")
            raise
        finally:
            logger.info(f"Shutting down producer... Total events produced: {events_produced}")
            self.producer.close()

def main():
    """Main function to run the producer"""
    try:
        # Add these debug lines
        load_dotenv()
        print("Raw value:", repr(os.getenv("PRODUCTION_INTERVAL")))
        
        producer = KafkaLogisticsProducer()
        producer.produce_events()
    except Exception as e:
        logger.error(f"Failed to start producer: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
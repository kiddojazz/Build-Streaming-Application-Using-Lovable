import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer
from pythonjsonlogger import jsonlogger

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(timestamp)s %(level)s %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

class LogisticsConsumer:
    """Kafka consumer that processes logistics events and stores them in Elasticsearch"""
    
    def __init__(self):
        self.topic = os.getenv("KAFKA_TOPIC", "eagle_transport")
        self.group_id = os.getenv("KAFKA_CONSUMER_GROUP", "logistics_consumer_group")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.elasticsearch_host = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
        self.index_name = os.getenv("ELASTICSEARCH_INDEX", "logistics_events")
        self.batch_size = int(os.getenv("BATCH_SIZE", "100"))
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Initialize Elasticsearch client
        self.es = Elasticsearch([self.elasticsearch_host])
        
        logger.info(f"Initialized consumer for topic: {self.topic}")
    
    def _transform_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Transform event before insertion if needed"""
        # Convert string timestamps to datetime objects
        for field in ['timestamp', 'estimated_delivery', 'actual_delivery']:
            if event.get(field):
                event[field] = datetime.fromisoformat(event[field].replace('Z', '+00:00'))
        return event
    
    def _bulk_insert_to_elasticsearch(self, events: list) -> None:
        """Insert multiple events into Elasticsearch using bulk API"""
        actions = [
            {
                "_index": self.index_name,
                "_source": event
            }
            for event in events
        ]
        
        try:
            success, failed = helpers.bulk(
                self.es,
                actions,
                stats_only=True
            )
            logger.info(f"Bulk insert completed. Success: {success}, Failed: {failed}")
        except Exception as e:
            logger.error(f"Error during bulk insert: {str(e)}")
            raise
    
    def process_messages(self) -> None:
        """Process messages from Kafka and store them in Elasticsearch"""
        try:
            batch = []
            
            for message in self.consumer:
                try:
                    event = self._transform_event(message.value)
                    batch.append(event)
                    
                    # Process batch when it reaches the specified size
                    if len(batch) >= self.batch_size:
                        self._bulk_insert_to_elasticsearch(batch)
                        self.consumer.commit()
                        batch = []
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            
        finally:
            # Process any remaining events in the batch
            if batch:
                self._bulk_insert_to_elasticsearch(batch)
                self.consumer.commit()
            
            self.consumer.close()
            logger.info("Consumer shutdown complete")

def main():
    """Main function to run the consumer"""
    try:
        consumer = LogisticsConsumer()
        consumer.process_messages()
    except Exception as e:
        logger.error(f"Failed to start consumer: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
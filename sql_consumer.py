import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List

import pyodbc
from dotenv import load_dotenv
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

class LogisticsSQLConsumer:
    """Kafka consumer that processes logistics events and stores them in Azure SQL Database"""
    
    def __init__(self):
        self.topic = os.getenv("KAFKA_TOPIC", "eagle_transport")
        self.group_id = os.getenv("KAFKA_CONSUMER_GROUP", "logistics_sql_consumer_group")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.batch_size = int(os.getenv("BATCH_SIZE", "100"))
        
        # Initialize Kafka consumer with same settings as working Elasticsearch consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Initialize database connection
        self.conn = self._create_connection()
        self.cursor = self.conn.cursor()
        
        logger.info(f"Initialized SQL consumer for topic: {self.topic}")
    
    def _create_connection(self):
        """Create database connection with retry logic"""
        connection_string = (
            f"Driver={{{os.getenv('SQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
            f"Server={os.getenv('SQL_SERVER')};"
            f"Database={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USERNAME')};"
            f"PWD={os.getenv('SQL_PASSWORD')};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        
        try:
            connection = pyodbc.connect(connection_string)
            logger.info("Successfully connected to Azure SQL Database")
            return connection
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def _transform_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Transform event before insertion if needed"""
        # Convert string timestamps to datetime objects
        for field in ['timestamp', 'estimated_delivery', 'actual_delivery']:
            if event.get(field):
                event[field] = datetime.fromisoformat(event[field].replace('Z', '+00:00'))
        return event
    
    def _insert_logistics_event(self, event: Dict[str, Any]) -> None:
        """Insert a single logistics event and related data"""
        try:
            # Insert main event
            self.cursor.execute("""
                INSERT INTO logistics_events (
                    event_id, timestamp, event_type, shipment_id, tracking_number,
                    vehicle_id, driver_id, status, estimated_delivery, actual_delivery,
                    weather_conditions, delay_minutes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event['event_id'],
                event['timestamp'],
                event['event_type'],
                event['shipment_id'],
                event['tracking_number'],
                event['vehicle_id'],
                event['driver_id'],
                event['status'],
                event['estimated_delivery'],
                event['actual_delivery'],
                event['weather_conditions'],
                event['delay_minutes']
            ))
            
            # Insert locations
            for location_type, location_data in [('origin', event['origin']), ('destination', event['destination'])]:
                self.cursor.execute("""
                    INSERT INTO logistics_locations (
                        event_id, location_type, address, city, state, postal_code, country
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['event_id'],
                    location_type,
                    location_data['address'],
                    location_data['city'],
                    location_data['state'],
                    location_data['postal_code'],
                    location_data['country']
                ))
            
            # Insert package details
            self.cursor.execute("""
                INSERT INTO package_details (
                    event_id, weight, length, width, height,
                    category, handling_instructions, vehicle_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event['event_id'],
                event['package_details']['weight'],
                event['package_details']['dimensions']['length'],
                event['package_details']['dimensions']['width'],
                event['package_details']['dimensions']['height'],
                event['package_details']['category'],
                event['package_details']['handling_instructions'],
                event['package_details']['vehicle_type']
            ))
            
        except Exception as e:
            logger.error(f"Error inserting event {event['event_id']}: {str(e)}")
            raise
    
    def process_messages(self) -> None:
        """Process messages from Kafka and store them in SQL Database"""
        try:
            batch = []
            
            for message in self.consumer:
                try:
                    event = self._transform_event(message.value)
                    batch.append(event)
                    
                    # Process batch when it reaches the specified size
                    if len(batch) >= self.batch_size:
                        self._process_batch(batch)
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
                self._process_batch(batch)
                self.consumer.commit()
            
            self.consumer.close()
            self.cursor.close()
            self.conn.close()
            logger.info("Consumer shutdown complete")
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of events"""
        try:
            for event in batch:
                self._insert_logistics_event(event)
            
            self.conn.commit()
            logger.info(f"Successfully processed batch of {len(batch)} events")
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            self.conn.rollback()
            raise

def main():
    """Main function to run the consumer"""
    try:
        consumer = LogisticsSQLConsumer()
        consumer.process_messages()
    except Exception as e:
        logger.error(f"Failed to start consumer: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_logistics_index():
    """Create Elasticsearch index with proper mappings for logistics data"""
    
    # Initialize Elasticsearch client
    es = Elasticsearch([os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")])
    
    # Define index name
    index_name = "logistics_events"
    
    # Define mapping
    mapping = {
        "mappings": {
            "properties": {
                "event_id": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "event_type": {"type": "keyword"},
                "shipment_id": {"type": "keyword"},
                "tracking_number": {"type": "keyword"},
                "vehicle_id": {"type": "keyword"},
                "driver_id": {"type": "keyword"},
                "origin": {
                    "properties": {
                        "address": {"type": "text"},
                        "city": {"type": "keyword"},
                        "state": {"type": "keyword"},
                        "postal_code": {"type": "keyword"},
                        "country": {"type": "keyword"}
                    }
                },
                "destination": {
                    "properties": {
                        "address": {"type": "text"},
                        "city": {"type": "keyword"},
                        "state": {"type": "keyword"},
                        "postal_code": {"type": "keyword"},
                        "country": {"type": "keyword"}
                    }
                },
                "status": {"type": "keyword"},
                "estimated_delivery": {"type": "date"},
                "actual_delivery": {"type": "date"},
                "package_details": {
                    "properties": {
                        "weight": {"type": "float"},
                        "dimensions": {
                            "properties": {
                                "length": {"type": "integer"},
                                "width": {"type": "integer"},
                                "height": {"type": "integer"}
                            }
                        },
                        "category": {"type": "keyword"},
                        "handling_instructions": {"type": "text"},
                        "vehicle_type": {"type": "keyword"}
                    }
                },
                "weather_conditions": {"type": "keyword"},
                "delay_minutes": {"type": "integer"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    
    # Create index with mapping
    try:
        if es.indices.exists(index=index_name):
            print(f"Index {index_name} already exists. Deleting...")
            es.indices.delete(index=index_name)
        
        es.indices.create(index=index_name, body=mapping)
        print(f"Successfully created index {index_name} with mappings")
        
    except Exception as e:
        print(f"Error creating index: {str(e)}")
        raise

if __name__ == "__main__":
    create_logistics_index()
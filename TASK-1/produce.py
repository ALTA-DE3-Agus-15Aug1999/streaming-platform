from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroProducer
from datetime import datetime
import random
from config import TOPIC, SCHEMA_REGISTRY_URL

def produce():
    # Konfigurasi klien Schema Registry
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Definisikan skema
    value_schema_str = """
    {
      "type": "record",
      "name": "Stock",
      "fields": [
        {"name": "event_time", "type": "string"},
        {"name": "ticker", "type": "string"},
        {"name": "price", "type": "float"}
      ]
    }
    """

    # Mendaftar skema
    schema_id = schema_registry_client.register_schema(f'{TOPIC}-value', value_schema_str)

    # Membuat AvroProducer
    avro_producer = AvroProducer(
        {
            'bootstrap.servers': 'localhost:19092',
            'schema.registry.url': SCHEMA_REGISTRY_URL
        },
        schema_registry_client=schema_registry_client
    )

    try:
        while True:
            stock = {
                'event_time': datetime.now().isoformat(),
                'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
                'price': round(random.random() * 100, 2)
            }
            avro_producer.produce(topic=TOPIC, value=stock)
            avro_producer.poll(0)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        avro_producer.flush()

def main():
    produce()

if __name__ == "__main__":
    main()

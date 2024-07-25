from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroConsumer
from config import TOPIC, SCHEMA_REGISTRY_URL
from confluent_kafka import KafkaError, KafkaException

def consume():
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Konfigurasi AvroConsumer
    avro_consumer = AvroConsumer(
        {
            'bootstrap.servers': 'localhost:19092',
            'group.id': 'python-consumer',
            'auto.offset.reset': 'earliest',
            'schema.registry.url': SCHEMA_REGISTRY_URL
        },
        schema_registry_client=schema_registry_client
    )

    avro_consumer.subscribe([TOPIC])

    try:
        while True:
            msg = avro_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                print(f"Unhandled error: {msg.error()}")
                print(f'Received message: {msg.value()}')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        avro_consumer.close()

def main():
    consume()

if __name__ == "__main__":
    main()

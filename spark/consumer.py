from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
import json
import time


KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transactions"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "fraudDB"
COLLECTION_NAME = "transactions"


def classify_transaction(payload: dict) -> str:
    """Return a simple fraud label when no model-serving layer exists."""
    amount = float(payload.get("amount", 0))
    return "FRAUD" if amount >= 30000 else "NORMAL"


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "fraud-consumer-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[DB_NAME][COLLECTION_NAME]

    print("Consumer started. Waiting for Kafka messages...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                # Brokers can briefly return topic/partition errors before metadata settles.
                if msg.error().code() in (KafkaError._UNKNOWN_TOPIC, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            payload = json.loads(msg.value().decode("utf-8"))
            status = classify_transaction(payload)
            record = {
                **payload,
                "status": status,
                "processed_at": int(time.time()),
            }

            collection.insert_one(record)
            print(f"Saved transaction: amount={record['amount']}, status={status}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()
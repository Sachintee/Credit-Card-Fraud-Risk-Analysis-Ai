from confluent_kafka import Producer
import json
import time
import random

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print("Sent:", msg.value().decode())

print("Sending data...")

while True:
    data = {
        "amount": random.randint(100, 50000),
        "location": random.choice(["Delhi", "Mumbai", "Jaipur"]),
        "time": random.randint(1, 100000)
    }

    p.produce('transactions', json.dumps(data), callback=delivery_report)
    p.flush()

    time.sleep(2)
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ["Mumbai", "Pune", "Delhi", "Bangalore"]

while True:
    data = {
        "user_id": random.randint(1, 1000),
        "city": random.choice(cities),
        "amount": round(random.uniform(100, 5000), 2),
        "timestamp": time.time()
    }

    producer.send("transactions", value=data)
    print(f"Sent: {data}")

    time.sleep(2)

import json
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topic = os.environ.get("KAFKA_TOPIC", "retail-orders")
username = os.environ.get("KAFKA_USERNAME", "$ConnectionString")
password = os.environ["KAFKA_PASSWORD"]

producer = KafkaProducer(
    bootstrap_servers=bootstrap,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=username,
    sasl_plain_password=password,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    order_id = f"ORD-{random.randint(100000, 999999)}"
    event = {
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(1, 20):04d}",
        "product_id": f"P-{random.randint(1, 10):03d}",
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 500), 2),
        "order_ts": datetime.now(timezone.utc).isoformat()
    }

    producer.send(topic, key=order_id.encode(), value=event)
    producer.flush()
    print(event)
    time.sleep(2)

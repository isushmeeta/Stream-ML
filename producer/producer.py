import json
import time
import random
from confluent_kafka import Producer

# Connect to all 3 brokers — 1 is down, the others handle it
producer = Producer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'acks': 'all',          # wait for ALL in-sync replicas to confirm
    'retries': 5,           # retry on transient failures
})

def generate_transaction():
    """Simulate a fraud detection event"""
    return {
        'transaction_id': f"txn_{random.randint(10000, 99999)}",
        'user_id': f"user_{random.randint(1, 1000)}",
        'amount': round(random.uniform(1.0, 5000.0), 2),
        'merchant': random.choice(['amazon', 'netflix', 'random_casino', 'grocery_store']),
        'timestamp': time.time()
    }

def delivery_report(err, msg):
    """Called when message is confirmed delivered (or failed)"""
    if err:
        print(f"  DELIVERY FAILED: {err}")
    else:
        print(f"  Delivered to {msg.topic()} partition [{msg.partition()}] offset {msg.offset()}")

print("Starting producer — sending 20 transactions...")
for i in range(100):
    transaction = generate_transaction()
    
    producer.produce(
        topic='transactions',
        key=transaction['user_id'],        # same user_id always goes to same partition
        value=json.dumps(transaction),
        callback=delivery_report
    )
    
    producer.poll(0)    # trigger delivery callbacks without blocking
    time.sleep(0.5)

producer.flush()        # wait for all messages to be confirmed
print("\nAll messages delivered.")





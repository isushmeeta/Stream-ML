import json
import sys
from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'ml-inference-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

consumer.subscribe(['transactions'])

print("Consumer started. Waiting for messages...", flush=True)  # ← flush=True
sys.stdout.flush()

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            print(".", end="", flush=True)  # ← prints a dot every second so you know it's alive
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"\nEnd of partition {msg.partition()}", flush=True)
            else:
                print(f"\nERROR: {msg.error()}", flush=True)
            continue

        transaction = json.loads(msg.value().decode('utf-8'))
        print(f"\nProcessing txn {transaction['transaction_id']} | "
              f"user={transaction['user_id']} | "
              f"amount=${transaction['amount']} | "
              f"partition={msg.partition()} offset={msg.offset()}", flush=True)

        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    print("\nShutting down consumer...", flush=True)
finally:
    consumer.close()
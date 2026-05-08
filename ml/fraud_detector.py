import os
import ray
import joblib
import json
import time
import numpy as np
from confluent_kafka import Consumer, KafkaError
from ray.exceptions import RayActorError

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
os.environ["JAVA_HOME"] = "C:\\Users\\User\\AppData\\Local\\Programs\\Eclipse Adoptium\\jdk-11.0.30.7-hotspot"


@ray.remote(max_restarts=3)
class FraudDetectorWorker:
    def __init__(self,worker_id):
        self.worker_id=worker_id
        self.model=joblib.load("ml/models/fraud_model_IEEE.pkl")
        self.feature_cols=joblib.load("ml/models/feature_cols_IEEE.pkl")
        self.predictions_made=0
        print(f"Worker {worker_id} ready - IEEE model loaded", flush=True)

    def predict(self, transaction):
        start=time.time()

        features={

            'TransactionAmt': transaction.get('amount', 0),
            'card1': transaction.get('card1', -999),
            'card2': transaction.get('card2', -999),
            'card3': transaction.get('card3', -999),
            'card5': transaction.get('card5', -999),
            'addr1': transaction.get('addr1', -999),
            'addr2': transaction.get('addr2', -999),
            'C1': transaction.get('C1',  -999),
            'C2': transaction.get('C2',  -999),
            'C6': transaction.get('C6',  -999),
            'C11': transaction.get('C11', -999),
            'C13': transaction.get('C13', -999),
            'C14': transaction.get('C14', -999),
            'D1': transaction.get('D1',  -999),
            'D10': transaction.get('D10', -999),
            'D15': transaction.get('D15', -999),

        }
        X=np.array([[features[col] for col in self.feature_cols]])
        fraud_prob=self.model.predict_proba(X)[0][1]
        is_fraud=fraud_prob> 0.5 
        latency_ms=(time.time()-start)*1000
        self.predictions_made +=1
        
        result={
            'transaction_id':    transaction['transaction_id'],
            'user_id':           transaction['user_id'],
            'amount':            transaction['amount'],
            'fraud_probability': round(fraud_prob, 4),
            'is_fraud':          bool(is_fraud),
            'latency_ms':        round(latency_ms, 2),
            'worker_id':         self.worker_id,
        }
        if is_fraud:
            print(f"Fraud Alert [worker-{self.worker_id}]"
                  f"txn={transaction['transaction_id']}"
                  f"user={transaction['user_id']}"
                  f"amount=${transaction['amount']}"
                  f"prob={fraud_prob:.3f}", flush=True)
                  
        return result

    def health_check(self):
        return{"worker_id": self.worker_id, "alive":True}       

def get_healthy_worker(workers,worker_index,num_workers):
    for attempt in range(num_workers):
        idx=(worker_index+attempt) % num_workers
        try:
            ray.get(workers[idx].health_check.remote(),timeout=2.0)
            return workers[idx],idx
        except (RayActorError, ray.exceptions.GetTimeoutError):
            print(f"Worker {idx} dead — replacing...", flush=True)
            workers[idx] = FraudDetectorWorker.remote(idx)
            return workers[idx], idx
    return workers[worker_index % num_workers], worker_index % num_workers


def run_inference_pipeline(num_workers=3):
    ray.init(num_cpus=num_workers, ignore_reinit_error=True)
    print(f"Ray started with {num_workers} workers", flush=True)

    workers = [FraudDetectorWorker.remote(i) for i in range(num_workers)]
    print(f"Created {num_workers} fraud detector workers\n", flush=True)

    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
        'group.id':'ray-inference-group',
        'auto.offset.reset':'latest',
        'enable.auto.commit': False,
    })
    consumer.subscribe(['transactions'])

    print("Inference pipeline running. Waiting for transactions...\n", flush=True)

    worker_index = 0
    results = []
    latencies = []

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print(".", end="", flush=True)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"ERROR: {msg.error()}", flush=True)
                continue

            transaction = json.loads(msg.value().decode('utf-8'))

            worker, worker_index = get_healthy_worker(
                workers, worker_index, num_workers
            )
            worker_index += 1

            try:
                future = worker.predict.remote(transaction)
                result = ray.get(future, timeout=5.0)

                results.append(result)
                latencies.append(result['latency_ms'])

                if len(results) % 20 == 0:
                    fraud_count = sum(1 for r in results if r['is_fraud'])
                    avg_lat = sum(latencies[-20:]) / 20
                    print(f"\nStats: {len(results)} predictions | "
                          f"{fraud_count} fraud alerts | "
                          f"avg latency:{avg_lat:.2f}ms", flush=True)

                consumer.commit(asynchronous=False)

            except RayActorError:
                print(f"\nWorker crashed — message will be reprocessed", flush=True)
            except ray.exceptions.GetTimeoutError:
                print(f"\nWorker timeout — message will be reprocessed", flush=True)

    except KeyboardInterrupt:
        print("\nShutting down...", flush=True)
        if results:
            fraud_count = sum(1 for r in results if r['is_fraud'])
            avg_lat = sum(latencies) / len(latencies)
            p99 = sorted(latencies)[int(len(latencies) * 0.99)]
            print(f"\nFinal stats:")
            print(f"Total predictions:{len(results)}")
            print(f"Fraud detected:{fraud_count} ({fraud_count/len(results):.1%})")
            print(f"Avg latency:{avg_lat:.2f}ms")
            print(f"p99 latency:{p99:.2f}ms")
    finally:
        consumer.close()
        ray.shutdown()


if __name__ == "__main__":
    run_inference_pipeline(num_workers=3) 
        





















































































































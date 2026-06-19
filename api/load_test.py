import httpx 
import asyncio
import time
import random
import json

API_URL = "http://localhost:8000"


def generate_transation():
    return{
        "user_id":f"user_{random.randint(1,1000)}",
        "amount" : round(random.uniform(1.0,5000.0), 2),
        "card1" : random.randint(100, 9999),
        "card2" :random.randint(100,600),
        "C1": random.randint(0,10),
        "C2":random.randint(0,10),
        "D1": random.randint(0,500),

        }

async def send_request(client,semaphore):
    async with semaphore:
        txn=generate_transation()
        start=time.time()
        try:
            response=await client.post(
                f"{API_URL}/predict",
                json=txn,
                timeout=30

            )
            latency=(time.time()-start)*1000
            return {"success": True, "latency_ms":latency,"status":response.status_code}
        except Exception as e:
            return {"success": False, "error":str(e)}
async def load_test(num_requests=50, concurrency=3):
    print(f"Load test: {num_requests} requests, {concurrency} concurrent")
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:
        start = time.time()
        tasks = [send_request(client, semaphore)
                 for _ in range(num_requests)]
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start

    successful = [r for r in results if r.get('success')]
    latencies = [r['latency_ms'] for r in successful]
    latencies.sort()

    print(f"\nResults:")
    print(f" Total requests: {num_requests}")
    print(f" Successful: {len(successful)}")
    print(f" Total time: {total_time:.2f}s")
    print(f" Throughput: {len(successful)/total_time:.1f} req/sec")
    print(f" Avg latency: {sum(latencies)/len(latencies):.2f}ms")
    print(f" p50 latency: {latencies[int(len(latencies)*0.50)]:.2f}ms")
    print(f" p95 latency: {latencies[int(len(latencies)*0.95)]:.2f}ms")
    print(f" p99 latency: {latencies[int(len(latencies)*0.99)]:.2f}ms")
    failed = [r for r in results if not r.get('success')]
    if failed:
        print(f"\nFirst failure: {failed[0]}")

if __name__ == "__main__":
    asyncio.run(load_test(num_requests=200, concurrency=20))

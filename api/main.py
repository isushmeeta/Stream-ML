import os 
import sys
import time 
import json
import uuid
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

import ray
import redis
import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request 
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
from confluent_kafka import Producer

os.environ["HADOOP_HOME"]= "C:\\hadoop"
os.environ["PATH"]=os.environ["PATH"]+";C:\\hadoop\\bin"
os.environ["JAVA_HOME"]="C:\\Users\\user\\AppData\\Local\\Programs\\Eclipse Adoptium\\jdk-11.0.30.7-hotspot"

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

@ray.remote(max_restarts=3)
class FraudDetectorWorker:
    def __init__(self, worker_id:int):
        self.worker_id=worker_id
        self.model=joblib.load("ml/models/fraud_model_IEEE.pkl")
        self.feature_cols=joblib.load("ml/models/feature_cols_IEEE.pkl")
        self.predictions_made=0
        self.total_latency=0.0
        logger.info(f"Worker{worker_id} ready")
    

    def predict(self, transaction:dict) -> dict:
        start =time.time()

        features ={
            'TransactionAmt': transaction.get('amount',0),
            'TransactionDT': transaction.get('timestamp',0),
            'card1': transaction.get('card1',  -999),
            'card2':  transaction.get('card2',  -999),
            'card3':  transaction.get('card3',  -999),
            'card4':  transaction.get('card4',  -999),
            'card5':  transaction.get('card5',  -999),
            'card6':  transaction.get('card6',  -999),
            'addr1':  transaction.get('addr1',  -999),
            'addr2':  transaction.get('addr2',  -999),
            'dist1':  transaction.get('dist1',  -999),
            'dist2':  transaction.get('dist2',  -999),
            'P_emaildomain': transaction.get('P_emaildomain', -999),
            'R_emaildomain': transaction.get('R_emaildomain', -999),
            'C1':  transaction.get('C1',  -999),
            'C2':  transaction.get('C2',  -999),
            'C3':  transaction.get('C3',  -999),
            'C4':  transaction.get('C4',  -999),
            'C5':  transaction.get('C5',  -999),
            'C6':  transaction.get('C6',  -999),
            'C7':  transaction.get('C7',  -999),
            'C8':  transaction.get('C8',  -999),
            'C9':  transaction.get('C9',  -999),
            'C10': transaction.get('C10', -999),
            'C11': transaction.get('C11', -999),
            'C13': transaction.get('C13', -999),
            'C14': transaction.get('C14', -999),
            'D1':  transaction.get('D1',  -999),
            'D2':  transaction.get('D2',  -999),
            'D3':  transaction.get('D3',  -999),
            'D4':  transaction.get('D4',  -999),
            'D5':  transaction.get('D5',  -999),
            'D6':  transaction.get('D6',  -999),
            'D10': transaction.get('D10', -999),
            'D11': transaction.get('D11', -999),
            'D15': transaction.get('D15', -999),

        }

        features['amt_log']=np.log1p(features['TransactionAmt'])
        features['hour']=(features['TransactionDT']/3600)%24
        features['day']=(features['TransactionDT']/(3600*24))%7


        X=pd.DataFrame([features]) [self.feature_cols]
        fraud_prob= self.model.predict_proba(X)[0][1]
        is_fraud=bool(fraud_prob>0.5)
        latency_ms=(time.time()-start)*1000

        self.predictions_made +=1
        self.total_latency += latency_ms

        return{
            'fraud_probabilty': round(float(fraud_prob),4),
            'is_fraud': is_fraud,
            'latency_ms':round(latency_ms,2),
            'worker_id': self.worker_id,
        }
    def get_stats(self) -> dict:
        avg_lat=self.total_latency /max(self.predictions_made,1)
        return{
            'worker_id': self.worker_id,
            'predictions_made': self.predictions_made,
            'avg_latency_ms': round(avg_lat,2)
        }
workers=[]
redis_client= None
kafka_producer=None
request_count=0
fraud_count=0
latencies=[]
start_time=time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global workers, redis_client,kafka_producer

    ray.init(num_cpus=3,ignore_reinit_error=True)
    workers=[FraudDetectorWorker.remote(i) for i in range(3)]
    logger.info("Ray workers started")


    #redis connection build
    redis_client=redis.Redis(host='localhost', port=6379,db=0)
    redis_client.ping()
    logger.info("Redis Conneected")

    #kafka producer for async logging

    kafka_producer=Producer({
        'bootstrap.servers': 'localhost:9092, localhost: 9093, localhost:9094','acks':1,    
        })
    logger.info("Kafka producer ready")
    logger.info("StreamML API ready")

    yield

    ray.shutdown()
    logger.info("Shutdown complete")

app=FastAPI(
    title="StreamML fraud detection API",
    description="Real time fraud detection -Xboost model, ray workers, IEEE-CIS dataset",
    version ="1.0.0",
    lifespan=lifespan,

)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],

)

#request and response 
class TransactionRequest(BaseModel):
    transaction_id: str = Field(default_factory=lambda: f"txn_{uuid.uuid4().hex[:8]}")
    user_id: str
    amount: float = Field(gt=0, description="Transaction amount in USD")
    timestamp: Optional[float] = Field(default_factory=time.time)
    card1: Optional[int] = -999
    card2: Optional[int] = -999
    card3: Optional[int] = -999
    card4: Optional[int] = -999
    card5: Optional[int] = -999
    card6: Optional[int] = -999
    addr1: Optional[int] = -999
    addr2: Optional[int] = -999
    C1: Optional[int] = -999
    C2: Optional[int] = -999
    C6: Optional[int] = -999
    C11: Optional[int] = -999
    C13: Optional[int] = -999
    C14: Optional[int] = -999
    D1: Optional[int] = -999
    D10: Optional[int] = -999
    D15: Optional[int] = -999

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_42",
                "amount": 4981.64,
                "card1": 9500,
                "C1": 3,
                "D1": 14
            }
        }

class PredictionResponse(BaseModel):
    transaction_id:str
    user_id: str
    amount: float
    fraud_probability: float
    is_fraud: bool
    risk_level: str
    latency_ms: float
    worker_id: int
    cached: bool
    timestamp: float    


def get_risk_level(prob: float) -> str:
    if prob >= 0.8:   return "CRITICAL"
    if prob >= 0.5:   return "HIGH"
    if prob >= 0.3:   return "MEDIUM"
    return "LOW"


def log_to_kafka(result: dict):
    try:
        kafka_producer.produce(
            topic='predictions',
            key=result['transaction_id'],
            value=json.dumps(result).encode('utf-8'),
        )
        kafka_producer.poll(0)
    except Exception as e:
        logger.error(f"Kafka log error: {e}")

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "uptime_seconds": round(time.time() - start_time, 1),
        "workers": 3,
        "redis": "connected",
    }

@app.post("/predict", response_model=PredictionResponse)
async def predict(
    transaction: TransactionRequest,
    background_tasks: BackgroundTasks,
    request: Request,
):
    global request_count, fraud_count, latencies
    request_count += 1
    req_start = time.time()

    # Check Redis cache first
    cache_key = f"pred:{transaction.transaction_id}"
    cached = redis_client.get(cache_key)
    if cached:
        result = json.loads(cached)
        result['cached'] = True
        return PredictionResponse(**result)

    # Round-robin worker selection
    worker = workers[request_count % 3]

    try:
        future = worker.predict.remote(transaction.model_dump())
        prediction = await asyncio.get_event_loop().run_in_executor(
            None, ray.get, future
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Inference failed: {str(e)}")

    total_latency = (time.time() - req_start) * 1000
    latencies.append(total_latency)
    if prediction['is_fraud']:
        fraud_count += 1

    result = {
        'transaction_id':    transaction.transaction_id,
        'user_id':           transaction.user_id,
        'amount':            transaction.amount,
        'fraud_probability': prediction['fraud_probability'],
        'is_fraud':          prediction['is_fraud'],
        'risk_level':        get_risk_level(prediction['fraud_probability']),
        'latency_ms':        round(total_latency, 2),
        'worker_id':         prediction['worker_id'],
        'cached':            False,
        'timestamp':         time.time(),
    }

    #cache in redis for 5 minutes
    redis_client.setex(cache_key, 300, json.dumps(result))

    #Log to Kafka asynchronously — doesn't block response
    background_tasks.add_task(log_to_kafka, result)

    return PredictionResponse(**result)


@app.post("/predict/batch")
async def predict_batch(
    transactions: list[TransactionRequest],
    background_tasks: BackgroundTasks,
):
    if len(transactions) > 100:
        raise HTTPException(status_code=400, detail="Max 100 per batch")

    results = []
    futures = []

    for i, txn in enumerate(transactions):
        worker = workers[i % 3]
        futures.append((txn, worker.predict.remote(txn.model_dump())))

    for txn, future in futures:
        try:
            prediction = await asyncio.get_event_loop().run_in_executor(
                None, ray.get, future
            )
            results.append({
                'transaction_id':    txn.transaction_id,
                'fraud_probability': prediction['fraud_probability'],
                'is_fraud':          prediction['is_fraud'],
                'risk_level':        get_risk_level(prediction['fraud_probability']),
                'latency_ms':        prediction['latency_ms'],
            })
        except Exception as e:
            results.append({
                'transaction_id': txn.transaction_id,
                'error': str(e)
            })

    return {"predictions": results, "count": len(results)}

@app.get("/metrics")
async def metrics():
    p99 = sorted(latencies)[int(len(latencies) * 0.99)] if len(latencies) >= 100 else None
    avg = sum(latencies) / len(latencies) if latencies else 0

    worker_stats = []
    for w in workers:
        try:
            stats = ray.get(w.get_stats.remote(), timeout=2.0)
            worker_stats.append(stats)
        except Exception:
            worker_stats.append({"error": "worker unavailable"})

    return {
        "total_requests":   request_count,
        "total_fraud":      fraud_count,
        "fraud_rate":       round(fraud_count / max(request_count, 1), 4),
        "avg_latency_ms":   round(avg, 2),
        "p99_latency_ms":   p99,
        "uptime_seconds":   round(time.time() - start_time, 1),
        "worker_stats":     worker_stats,
    }


@app.get("/")
async def root():
    return {
        "service": "StreamML Fraud Detection API",
        "version": "1.0.0",
        "model":   "XGBoost IEEE-CIS AUC=0.9343",
        "docs":    "/docs",
        "health":  "/health",
        "predict": "/predict",
        "metrics": "/metrics",
    }


if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1,
    )
# StreamML — Distributed ML Inference Platform

> Real-time fraud detection pipeline built on distributed systems principles.
> Target: 50k predictions/sec | <10ms p99 latency | <15s node recovery

## Architecture

[Producer] → [Kafka: 3 brokers] → [Consumer Workers]
↓
[ML Inference] (Phase 4)
↓
[FastAPI + Monitoring] (Phase 5-6)


## Stack
- **Messaging:** Apache Kafka (KRaft mode, 3 brokers)
- **Workers:** Python confluent-kafka (Phase 2) → Ray (Phase 4)
- **Pipeline:** Apache Spark Streaming (Phase 3)
- **API:** FastAPI + Nginx (Phase 5)
- **Monitoring:** Prometheus + Grafana (Phase 6)
- **Deployment:** Docker Compose → Kubernetes (Phase 7-8)

## Phases
- [x] Phase 1 — Parallelism & concurrency fundamentals
- [x] Phase 2 — 3-broker Kafka cluster with fault tolerance
- [ ] Phase 3 — Spark streaming feature pipeline
- [ ] Phase 4 — Ray distributed ML workers
- [ ] Phase 5 — FastAPI + load balancer
- [ ] Phase 6 — Prometheus + Grafana monitoring
- [ ] Phase 7 — Kubernetes deployment

## Phase 2 — What was built
- 3-broker Kafka cluster in KRaft mode (no ZooKeeper)
- Topic: `transactions` — 3 partitions, replication factor 3
- Producer: generates synthetic fraud detection events, keys by `user_id`
- Consumer: `ml-inference-group`, manual offset commit, fault isolated
- **Fault tolerance demo:** killed broker mid-stream, zero message loss,
  automatic leader re-election, recovery under 15 seconds

## Fault Tolerance Numbers
| Scenario | Result |
|---|---|
| Kill 1 of 3 brokers mid-stream | Zero message loss |
| Broker recovery time | < 15 seconds |
| Min brokers needed for writes | 2 of 3 (MIN_INSYNC_REPLICAS) |

## Running Locally
```bash
# Start Kafka cluster
docker compose up -d

# Terminal 1 — consumer
python consumer/consumer.py

# Terminal 2 — producer
python producer/producer.py

# Kafka UI
open http://localhost:8080
```

## Project Structure

streamml/
├── docker-compose.yml      # 3-broker Kafka + UI
├── producer/
│   └── producer.py         # Transaction event generator
├── consumer/
│   └── consumer.py         # Fault-isolated consumer
└── README.md











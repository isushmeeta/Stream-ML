# Troubleshooting Guide

Real errors encountered building this project and exactly how to fix them.
This guide exists so you don't spend hours like me on the same problems.

---

## Phase 2 — Kafka

### Error: `the input device is not a TTY`
**Platform:** Windows Git Bash  
**Cause:** Git Bash doesn't support TTY mode for interactive Docker commands  
**Fix:** Prefix every `docker exec` command with `winpty`:
```bash
winpty docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092 ...
```

### Error: Consumer shows only dots, no messages
**Cause:** Consumer group has a saved offset from a previous run — it starts ahead of new messages  
**Fix:** Reset the offset, then restart consumer:
```bash
winpty docker exec -it kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --group ml-inference-group --topic transactions --reset-offsets --to-earliest --execute
```

### Error: All messages landing on partition 0 only
**Cause:** Topic was created with 1 partition instead of 3  
**Fix:** Delete and recreate with 3 partitions:
```bash
winpty docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --delete --topic transactions
winpty docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --create --topic transactions --partitions 3 --replication-factor 3
```

### Error: `bitnami/spark:3.5.0 not found`
**Cause:** Bitnami removed this specific tag from Docker Hub  
**Fix:** Use `apache/spark:3.5.1` instead — the official Apache image is more stable:
```yaml
image: apache/spark:3.5.1
```

---

## Phase 3 — Spark

### Error: `Java not found and JAVA_HOME is not set`
**Cause:** PySpark needs Java installed locally even when Spark runs in Docker  
**Fix:**
1. Download Java 11 from https://adoptium.net
2. Install with "Set JAVA_HOME" option checked
3. Open a fresh terminal — JAVA_HOME is now set automatically

### Error: `HADOOP_HOME and hadoop.home.dir are unset`
**Platform:** Windows only  
**Cause:** PySpark on Windows requires Hadoop winutils  
**Fix:** Download winutils and hadoop.dll:
```cmd
mkdir C:\hadoop\bin
python -c "import urllib.request; urllib.request.urlretrieve('https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/winutils.exe', 'C:/hadoop/bin/winutils.exe')"
python -c "import urllib.request; urllib.request.urlretrieve('https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/hadoop.dll', 'C:/hadoop/bin/hadoop.dll')"
```
Then add to top of your Python file:
```python
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
```

### Error: `Failed to find data source: kafka`
**Cause:** Spark needs the Kafka connector JAR — auto-download fails on Windows  
**Fix:** Download JARs manually:
```cmd
python -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar', 'C:/users/user/streamml/spark-kafka.jar')"
python -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar', 'C:/users/user/streamml/kafka-clients.jar')"
python -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar', 'C:/users/user/streamml/spark-token-provider.jar')"
python -c "import urllib.request; urllib.request.urlretrieve('https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar', 'C:/users/user/streamml/commons-pool2.jar')"
```
Then reference them in SparkSession:
```python
.config("spark.jars", "spark-kafka.jar,kafka-clients.jar,spark-token-provider.jar,commons-pool2.jar")
```

### Error: `UnsatisfiedLinkError: NativeIO$Windows.access0`
**Cause:** hadoop.dll missing from hadoop bin folder  
**Fix:** Download hadoop.dll (see HADOOP_HOME fix above) — both winutils.exe AND hadoop.dll are required.

### Warning: `Current batch is falling behind`
**Cause:** Laptop CPU is slower than the 10-second trigger interval  
**This is NOT an error** — Spark is still processing correctly, just slightly delayed.  
**Fix for production:** Increase worker resources or reduce trigger interval.

---

## General Windows + Git Bash Tips

### Never paste multiline commands into Git Bash
Git Bash handles multiline commands poorly. Always write commands on a single line.

### Always use `winpty` for interactive Docker commands
```bash
winpty docker exec -it CONTAINER COMMAND
```

### After laptop restart — run these before anything else
```bash
cd /c/users/user/streamml
export JAVA_HOME="/c/Users/User/AppData/Local/Programs/Eclipse Adoptium/jdk-11.0.30.7-hotspot"
export HADOOP_HOME="/c/hadoop"
export PATH="$PATH:/c/hadoop/bin"
docker compose up -d
```
# Uber End-to-End Data Engineering Pipeline

Real-time ride-event pipeline on Azure using the **Medallion (Bronze / Silver / Gold)** architecture. Built with simulated ride events (no real Uber data).

## Architecture
```
Event generator -> Azure Event Hubs (Kafka endpoint)
   -> Bronze (Structured Streaming -> Delta, ADLS Gen2)
   -> Silver (Delta Live Tables: cleaning, validation)
   -> Gold   (rides, revenue, driver KPIs) -> Power BI
Drivers dimension: SCD Type 2 using MERGE
```

## Stack
Azure Event Hubs, ADLS Gen2, Azure Databricks, PySpark, Structured Streaming, Delta Live Tables, Delta Lake (MERGE), SQL, Power BI

## 1. Event generator (`00_event_generator.py`)
```python
import json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

NAMESPACE = "<your-namespace>"
CONN_STR = "<event-hubs-connection-string>"

producer = KafkaProducer(
    bootstrap_servers=f"{NAMESPACE}.servicebus.windows.net:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONN_STR,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    event = {
        "ride_id": str(uuid.uuid4()),
        "driver_id": f"D{random.randint(1, 200):04d}",
        "rider_id": f"R{random.randint(1, 5000):05d}",
        "fare": round(random.uniform(-5, 80), 2),
        "distance_km": round(random.uniform(0.5, 30), 2),
        "pickup_lat": round(random.uniform(28.4, 28.9), 5),
        "pickup_lon": round(random.uniform(76.8, 77.5), 5),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    producer.send("ride-events", event)
    time.sleep(0.2)
```

## 2. Bronze streaming (`01_bronze_stream.py`)
```python
from pyspark.sql import functions as F

NAMESPACE = "<your-namespace>"
conn = dbutils.secrets.get("uber", "eh-conn")
jaas = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn}";')
BASE = "abfss://lake@<storage-account>.dfs.core.windows.net"

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{NAMESPACE}.servicebus.windows.net:9093")
    .option("subscribe", "ride-events")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .option("startingOffsets", "earliest")
    .load())

bronze = raw.select(F.col("value").cast("string").alias("raw_json"),
                    F.col("timestamp").alias("ingest_time"))

(bronze.writeStream.format("delta")
    .option("checkpointLocation", f"{BASE}/_checkpoints/bronze_rides")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start(f"{BASE}/bronze/rides"))
```

## 3. Silver with Delta Live Tables (`02_silver_dlt.py`)
```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

BASE = "abfss://lake@<storage-account>.dfs.core.windows.net"

schema = StructType([
    StructField("ride_id", StringType()), StructField("driver_id", StringType()),
    StructField("rider_id", StringType()), StructField("fare", DoubleType()),
    StructField("distance_km", DoubleType()), StructField("pickup_lat", DoubleType()),
    StructField("pickup_lon", DoubleType()), StructField("event_time", StringType()),
])

@dlt.view
def bronze_rides():
    return spark.readStream.format("delta").load(f"{BASE}/bronze/rides")

@dlt.table(name="silver_rides", comment="Cleaned and validated ride events")
@dlt.expect_or_drop("valid_ride_id", "ride_id IS NOT NULL")
@dlt.expect_or_drop("positive_fare", "fare > 0")
@dlt.expect_or_drop("valid_coordinates", "pickup_lat BETWEEN -90 AND 90 AND pickup_lon BETWEEN -180 AND 180")
def silver_rides():
    return (dlt.read_stream("bronze_rides")
        .select(F.from_json("raw_json", schema).alias("r"), "ingest_time")
        .select("r.*", "ingest_time")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .dropDuplicates(["ride_id"]))
```

## 4. Gold KPIs (`03_gold_aggregates.py`)
```python
from pyspark.sql import functions as F

silver = spark.read.table("silver_rides")

(silver.groupBy(F.window("event_time", "1 hour").alias("w"))
    .agg(F.count("*").alias("rides"), F.sum("fare").alias("revenue"),
         F.avg("distance_km").alias("avg_distance_km"))
    .select(F.col("w.start").alias("hour_start"), "rides", "revenue", "avg_distance_km")
    .write.format("delta").mode("overwrite").saveAsTable("gold_hourly_kpis"))

(silver.groupBy("driver_id")
    .agg(F.count("*").alias("rides"), F.sum("fare").alias("earnings"))
    .write.format("delta").mode("overwrite").saveAsTable("gold_driver_kpis"))
```

## 5. SCD Type 2 (`04_scd2_drivers.sql`)
```sql
MERGE INTO dim_driver AS t
USING (
  SELECT u.driver_id AS merge_key, u.* FROM stg_driver_updates u
  UNION ALL
  SELECT NULL AS merge_key, u.* FROM stg_driver_updates u
  JOIN dim_driver d ON u.driver_id = d.driver_id AND d.is_current = true
  WHERE u.city <> d.city OR u.vehicle_type <> d.vehicle_type
) AS s
ON t.driver_id = s.merge_key AND t.is_current = true
WHEN MATCHED AND (t.city <> s.city OR t.vehicle_type <> s.vehicle_type) THEN
  UPDATE SET t.is_current = false, t.effective_to = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (driver_id, city, vehicle_type, effective_from, effective_to, is_current)
  VALUES (s.driver_id, s.city, s.vehicle_type, s.updated_at, NULL, true);
```

## Data quality
Silver drops records with a null ride_id, a non-positive fare, or invalid coordinates (DLT `expect_or_drop`).

> Update the storage paths and secret names to match your workspace.

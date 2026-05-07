import os
os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:/hadoop/bin'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
import psycopg2
from datetime import datetime, timedelta

# In-memory store for impossible travel detection
user_location_history = {}

def setup_db():
    conn = psycopg2.connect(
        host="localhost", database="fraud_db",
        user="admin", password="admin123"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS validated_transactions (
            transaction_id TEXT PRIMARY KEY,
            user_id TEXT,
            timestamp TIMESTAMP,
            merchant_category TEXT,
            amount FLOAT,
            location TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            transaction_id TEXT PRIMARY KEY,
            user_id TEXT,
            timestamp TIMESTAMP,
            amount FLOAT,
            location TEXT,
            reason TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("DB tables ready.")

setup_db()

spark = SparkSession.builder \
    .appName("FraudDetector") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("location", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def check_impossible_travel(user_id, location, timestamp_str):
    """Returns (is_fraud, reason) based on impossible travel rule."""
    try:
        txn_time = datetime.fromisoformat(timestamp_str)
    except:
        txn_time = datetime.utcnow()

    if user_id in user_location_history:
        last = user_location_history[user_id]
        last_time = last["time"]
        last_location = last["location"]
        minutes_diff = (txn_time - last_time).total_seconds() / 60

        if minutes_diff <= 10 and last_location != location:
            reason = f"Impossible travel: {last_location} -> {location} within {minutes_diff:.1f} mins"
            user_location_history[user_id] = {"time": txn_time, "location": location}
            return True, reason

    user_location_history[user_id] = {"time": txn_time, "location": location}
    return False, ""

def process_batch(df, epoch_id):
    rows = df.collect()
    if not rows:
        return

    conn = psycopg2.connect(
        host="localhost", database="fraud_db",
        user="admin", password="admin123"
    )
    cur = conn.cursor()

    fraud_count = 0
    valid_count = 0

    for row in rows:
        is_fraud = False
        reason = ""

        # Rule 1: High value transaction
        if row.amount > 5000:
            is_fraud = True
            reason = f"High value transaction: ${row.amount}"

        # Rule 2: Impossible travel (only check if not already flagged)
        if not is_fraud:
            travel_fraud, travel_reason = check_impossible_travel(
                row.user_id, row.location, row.timestamp
            )
            if travel_fraud:
                is_fraud = True
                reason = travel_reason

        if is_fraud:
            fraud_count += 1
            print(f"  [FRAUD] {row.user_id} | ${row.amount} | {row.location} | {reason}")
            cur.execute("""
                INSERT INTO fraud_alerts
                (transaction_id, user_id, timestamp, amount, location, reason)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (row.transaction_id, row.user_id, row.timestamp,
                  row.amount, row.location, reason))
        else:
            valid_count += 1
            cur.execute("""
                INSERT INTO validated_transactions
                (transaction_id, user_id, timestamp, merchant_category, amount, location)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (row.transaction_id, row.user_id, row.timestamp,
                  row.merchant_category, row.amount, row.location))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Batch {epoch_id}: {valid_count} validated | {fraud_count} fraud flagged")

query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "C:/tmp/spark-checkpoint") \
    .start()

print("Spark streaming started. Watching for fraud...")
query.awaitTermination()
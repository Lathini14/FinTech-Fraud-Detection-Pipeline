import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

COUNTRIES = ['US', 'UK', 'AU', 'CA', 'SG', 'DE', 'FR', 'JP']
CATEGORIES = ['food', 'electronics', 'travel', 'clothing', 'entertainment', 'healthcare']

# Track last transaction per user for impossible travel detection
user_last_txn = {}

def generate_transaction():
    user_id = f"user_{random.randint(1, 20)}"
    now = datetime.utcnow()
    country = random.choice(COUNTRIES)

    # Occasionally inject fraud (1 in 8 chance)
    is_fraud_injection = random.random() < 0.05

    if is_fraud_injection:
        # High value transaction
        amount = round(random.uniform(5001, 15000), 2)
    else:
        amount = round(random.uniform(5, 800), 2)

    # Impossible travel: if user transacted recently in a different country
    if user_id in user_last_txn:
        last = user_last_txn[user_id]
        minutes_since = (now - last['time']).total_seconds() / 60
        if minutes_since < 10 and last['country'] != country and random.random() < 0.08:
            print(f"[FRAUD INJECT] Impossible travel for {user_id}")

    txn = {
        "user_id": user_id,
        "transaction_id": fake.uuid4(),
        "timestamp": now.isoformat(),
        "merchant_category": random.choice(CATEGORIES),
        "amount": amount,
        "location": country
    }

    user_last_txn[user_id] = {"time": now, "country": country}
    return txn

print("Producer started. Sending transactions every 0.5s...")
while True:
    txn = generate_transaction()
    producer.send('transactions', value=txn)
    print(f"Sent: {txn['user_id']} | ${txn['amount']} | {txn['location']}")
    time.sleep(0.5)
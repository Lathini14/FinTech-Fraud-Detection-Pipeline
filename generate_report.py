import psycopg2

conn = psycopg2.connect(
    host="localhost", database="fraud_db",
    user="admin", password="admin123"
)
cur = conn.cursor()

# Summary
cur.execute("SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM validated_transactions")
v = cur.fetchone()
validated_total, validated_count = v

cur.execute("SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM fraud_alerts")
f = cur.fetchone()
fraud_total, fraud_count = f

total_ingress = validated_total + fraud_total
total_count = validated_count + fraud_count
fraud_rate = (fraud_count / total_count * 100) if total_count > 0 else 0

# Fraud attempts by merchant category
# We estimate category from validated transactions proportionally per user
cur.execute("""
    SELECT
        vt.merchant_category,
        COUNT(DISTINCT fa.transaction_id) as fraud_attempts,
        ROUND(
            (COUNT(DISTINCT fa.transaction_id)::numeric / NULLIF(
                (SELECT COUNT(*) FROM fraud_alerts), 0
            ) * %s)::numeric, 2
        ) as estimated_fraud_amount
    FROM fraud_alerts fa
    JOIN (
        SELECT DISTINCT ON (user_id) user_id, merchant_category
        FROM validated_transactions
        ORDER BY user_id, timestamp DESC
    ) vt ON fa.user_id = vt.user_id
    GROUP BY vt.merchant_category
    ORDER BY fraud_attempts DESC
""", (float(fraud_total),))
fraud_by_category = cur.fetchall()

# Fraud by detection rule
cur.execute("""
    SELECT
        CASE
            WHEN reason LIKE 'Impossible travel%' THEN 'Impossible Travel'
            WHEN reason LIKE 'High value%' THEN 'High Value (>$5000)'
            ELSE 'Other'
        END as fraud_type,
        COUNT(*) as count,
        ROUND(SUM(amount)::numeric, 2) as total_amount
    FROM fraud_alerts
    GROUP BY fraud_type
    ORDER BY count DESC
""")
fraud_by_type = cur.fetchall()

cur.close()
conn.close()

# Print report
print("\n========================================")
print("   FRAUD DETECTION - ANALYTIC REPORT")
print("   Fraud Attempts by Merchant Category")
print("========================================")

print(f"\n--- PIPELINE SUMMARY ---")
print(f"{'Total Ingress Amount':<30} ${total_ingress:,.2f}")
print(f"{'Validated Amount':<30} ${validated_total:,.2f}")
print(f"{'Fraud Amount':<30} ${fraud_total:,.2f}")
print(f"{'Total Transactions':<30} {total_count}")
print(f"{'Fraud Transactions Flagged':<30} {fraud_count}")
print(f"{'Fraud Rate':<30} {fraud_rate:.1f}%")

print(f"\n--- FRAUD ATTEMPTS BY MERCHANT CATEGORY ---")
print(f"  (based on fraudulent users' most recent transaction category)")
print(f"{'Category':<20} {'Fraud Attempts':>15} {'Est. Fraud Amount ($)':>22}")
print("-" * 60)
if fraud_by_category:
    for row in fraud_by_category:
        print(f"{row[0]:<20} {row[1]:>15} {row[2]:>22,.2f}")
else:
    print("  No data yet.")

print(f"\n--- FRAUD BY DETECTION RULE ---")
print(f"{'Fraud Type':<25} {'Count':>8} {'Total Amount ($)':>18}")
print("-" * 54)
for row in fraud_by_type:
    print(f"{row[0]:<25} {row[1]:>8} {row[2]:>18,.2f}")

print("\n========================================\n")
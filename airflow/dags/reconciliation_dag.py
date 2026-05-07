from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def export_to_parquet():
    """Move validated transactions to Parquet (Data Warehouse layer)."""
    try:
        import pandas as pd
        conn = psycopg2.connect(
            host="postgres", database="fraud_db",
            user="admin", password="admin123"
        )
        df = pd.read_sql("SELECT * FROM validated_transactions", conn)
        conn.close()

        parquet_path = "/opt/airflow/data/validated_transactions.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"Exported {len(df)} validated transactions to Parquet.")
    except Exception as e:
        print(f"Parquet export failed: {e} — continuing without it.")

def run_reconciliation():
    conn = psycopg2.connect(
        host="postgres", database="fraud_db",
        user="admin", password="admin123"
    )
    cur = conn.cursor()

    cur.execute("SELECT COALESCE(SUM(amount), 0) FROM validated_transactions")
    validated_total = cur.fetchone()[0]

    cur.execute("SELECT COALESCE(SUM(amount), 0) FROM fraud_alerts")
    fraud_total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fraud_alerts")
    fraud_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM validated_transactions")
    validated_count = cur.fetchone()[0]

    total_ingress = validated_total + fraud_total
    total_count = fraud_count + validated_count
    fraud_rate = (fraud_count / total_count * 100) if total_count > 0 else 0

    cur.execute("""
        SELECT merchant_category, COUNT(*) as count,
               ROUND(SUM(amount)::numeric, 2) as total
        FROM validated_transactions
        GROUP BY merchant_category ORDER BY count DESC
    """)
    category_rows = cur.fetchall()

    cur.execute("""
        SELECT
            CASE
                WHEN amount BETWEEN 5000 AND 7000 THEN '$5,000 - $7,000'
                WHEN amount BETWEEN 7001 AND 10000 THEN '$7,001 - $10,000'
                WHEN amount BETWEEN 10001 AND 13000 THEN '$10,001 - $13,000'
                WHEN amount > 13000 THEN 'Above $13,000'
            END as amount_range,
            COUNT(*) as count,
            ROUND(SUM(amount)::numeric, 2) as total
        FROM fraud_alerts
        GROUP BY amount_range ORDER BY count DESC
    """)
    fraud_ranges = cur.fetchall()

    cur.execute("""
        SELECT user_id, COUNT(*) as fraud_count,
               ROUND(SUM(amount)::numeric, 2) as total_fraud_amount
        FROM fraud_alerts
        GROUP BY user_id ORDER BY fraud_count DESC LIMIT 5
    """)
    top_fraudsters = cur.fetchall()

    cur.close()
    conn.close()

    report_path = "/opt/airflow/data/reconciliation_report.csv"
    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["=== FRAUD DETECTION RECONCILIATION REPORT ==="])
        writer.writerow(["Generated at", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")])
        writer.writerow([])
        writer.writerow(["--- SUMMARY ---"])
        writer.writerow(["Total Ingress Amount ($)", round(total_ingress, 2)])
        writer.writerow(["Validated Amount ($)", round(validated_total, 2)])
        writer.writerow(["Fraud Amount ($)", round(fraud_total, 2)])
        writer.writerow(["Total Transactions", total_count])
        writer.writerow(["Validated Transactions", validated_count])
        writer.writerow(["Fraud Transactions Flagged", fraud_count])
        writer.writerow(["Fraud Rate (%)", round(fraud_rate, 2)])
        writer.writerow([])
        writer.writerow(["--- VALIDATED TRANSACTIONS BY MERCHANT CATEGORY ---"])
        writer.writerow(["Category", "Count", "Total ($)"])
        for row in category_rows:
            writer.writerow(row)
        writer.writerow([])
        writer.writerow(["--- FRAUD ALERTS BY AMOUNT RANGE ---"])
        writer.writerow(["Amount Range", "Count", "Total ($)"])
        for row in fraud_ranges:
            writer.writerow(row)
        writer.writerow([])
        writer.writerow(["--- TOP 5 FLAGGED USERS ---"])
        writer.writerow(["User ID", "Fraud Count", "Total Fraud Amount ($)"])
        for row in top_fraudsters:
            writer.writerow(row)

    print(f"Reconciliation report saved to {report_path}")
    print(f"Total Ingress: ${total_ingress:,.2f} | Validated: ${validated_total:,.2f} | Fraud Rate: {fraud_rate:.1f}%")

with DAG(
    dag_id='fraud_reconciliation',
    default_args=default_args,
    description='ETL: export Parquet + reconciliation report every 6 hours',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fraud', 'etl']
) as dag:

    export_parquet = PythonOperator(
        task_id='export_to_parquet',
        python_callable=export_to_parquet
    )

    reconcile = PythonOperator(
        task_id='run_reconciliation',
        python_callable=run_reconciliation
    )

    # Parquet export runs first, then reconciliation report
    export_parquet >> reconcile
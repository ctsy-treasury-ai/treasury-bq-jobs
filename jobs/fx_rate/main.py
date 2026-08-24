"""Daily sync of finance_treasury.fx_rate from CDP Impala fx_rates_dt.

Intended to run as a Cloud Run job. Fetches FX rates for the target date from
CDP, deletes any existing rows for that date in BigQuery, and inserts the fresh
rows.
"""
import os
import sys
from datetime import date, datetime, timedelta

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from common import bq, secrets  # noqa: E402

BQ_PROJECT = os.environ.get("BQ_PROJECT", "treasury-datamart-sandbox")
BQ_DATASET = os.environ.get("BQ_DATASET", "finance_treasury")
IMPALA_HOST = os.environ.get("IMPALA_HOST", "fdt-prod-datamart-master10.fdt-prod.bkje-jups.a5.cloudera.site")
IMPALA_USER = os.environ.get("IMPALA_USER", "jason.tse")
IMPALA_DB = os.environ.get("IMPALA_DB", "prd_crypto_treasury")


def fetch_cdp_fx(target_date: date, password: str):
    """Return list of (symbol, exchange_rate) for target_date from CDP."""
    from impala.dbapi import connect

    conn = connect(
        host=IMPALA_HOST,
        port=443,
        user=IMPALA_USER,
        password=password,
        database=IMPALA_DB,
        auth_mechanism="LDAP",
        use_http_transport=True,
        http_path="cliservice",
        use_ssl=True,
    )
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT `symbol`, exchange_rate
        FROM prd_crypto_treasury.fx_rates_dt
        WHERE `year`={target_date.year}
          AND `month`={target_date.month}
          AND `day`={target_date.day}
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def main() -> int:
    target_date_str = os.environ.get("TARGET_DATE")
    if target_date_str:
        target_date = date.fromisoformat(target_date_str)
    else:
        # Default to yesterday in UTC so we load the most recently completed day.
        target_date = datetime.utcnow().date() - timedelta(days=1)

    d_str = target_date.isoformat()
    print(f"[fx-rate-sync] Syncing FX rates for {d_str}")

    impala_password = secrets.get_secret("impala-password")
    rows = fetch_cdp_fx(target_date, impala_password)
    if not rows:
        print(f"  {d_str}: no data from CDP, nothing to sync")
        return 0

    print(f"  {d_str}: fetched {len(rows)} symbols from CDP")

    # Delete existing rows for the date
    bq.run_query(
        f"DELETE FROM `{BQ_PROJECT}.{BQ_DATASET}.fx_rate` WHERE Day_of_Quote_Time = '{d_str}'"
    )

    # Insert in batches
    batch_size = 500
    total_inserted = 0
    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start : batch_start + batch_size]
        values = []
        for symbol, rate in batch:
            safe_symbol = str(symbol).replace("'", "\\'")
            values.append(f"('{d_str}', '{safe_symbol}', {rate})")

        sql = f"""
            INSERT INTO `{BQ_PROJECT}.{BQ_DATASET}.fx_rate`
                (Day_of_Quote_Time, Symbol, Exchange_Rate)
            VALUES {', '.join(values)}
        """
        bq.run_query(sql)
        total_inserted += len(batch)

    print(f"  {d_str}: inserted {total_inserted} rows")

    # Verify
    data = bq.query_once(
        f"SELECT COUNT(*) AS cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.fx_rate` WHERE Day_of_Quote_Time = '{d_str}'"
    )
    if "rows" in data:
        print(f"  {d_str}: {data['rows'][0]['f'][0]['v']} rows in BQ")
    else:
        print(f"  Verify error: {data}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

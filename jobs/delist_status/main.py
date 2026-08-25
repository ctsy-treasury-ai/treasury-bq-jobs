"""Cloud Run Job: Sync delist_status from CDP to BigQuery.

Replaces the manual Google Sheet → Apps Script pipeline and the laptop-based
Windows Task Scheduler script. Runs daily via Cloud Scheduler.

Source: prd_restricted.ctsy_currency_mappings (Impala via CDP proxy)
Target: treasury-datamart-sandbox.finance_treasury.delist_status (BigQuery)
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from common import bq  # noqa: E402

PROJECT = os.environ.get("BQ_PROJECT", "treasury-datamart-sandbox")
DATASET = os.environ.get("BQ_DATASET", "finance_treasury")
TABLE = "delist_status"

CDP_HOST = os.environ.get(
    "CDP_HOST",
    "fdt-apse1-production-prd-impala-1e69f80ad671620d.elb.ap-southeast-1.amazonaws.com",
)
CDP_PORT = int(os.environ.get("CDP_PORT", "443"))
CDP_USER = os.environ.get("CDP_USER", "jason.tse")
CDP_PASSWORD = os.environ.get("CDP_PASSWORD", "")
CDP_DATABASE = os.environ.get("CDP_DATABASE", "prd_restricted")


def fetch_from_cdp() -> list[dict]:
    """Query CDP for delisted currencies. Returns list of {Currency, Delist_Status}."""
    from impala.dbapi import connect

    print(f"Connecting to CDP Impala: {CDP_HOST}/{CDP_DATABASE}")
    conn = connect(
        host=CDP_HOST,
        port=CDP_PORT,
        user=CDP_USER,
        password=CDP_PASSWORD,
        database=CDP_DATABASE,
        auth_mechanism="PLAIN",
        use_ssl=True,
        use_http_transport=True,
        http_path="fdt-prod-datamart/cdp-proxy-api/impala",
        timeout=60,
    )
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT reporting_currency, delist_status
            FROM prd_restricted.ctsy_currency_mappings
            WHERE delist_status != 'N/A'
              AND reporting_currency IS NOT NULL
            ORDER BY reporting_currency
        """)
        rows = cursor.fetchall()
        print(f"Fetched {len(rows)} delisted currencies from CDP")
        return [{"Currency": r[0], "Delist_Status": r[1]} for r in rows]
    finally:
        conn.close()


def load_to_bq(records: list[dict]) -> int:
    """Load NDJSON records into BigQuery via REST API multipart upload."""
    ndjson = "\n".join(json.dumps(r) for r in records)
    token = bq.access_token()

    boundary = "bq_load_boundary_001"
    metadata = {
        "configuration": {
            "load": {
                "destinationTable": {
                    "projectId": PROJECT,
                    "datasetId": DATASET,
                    "tableId": TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "schema": {
                    "fields": [
                        {"name": "Currency", "type": "STRING"},
                        {"name": "Delist_Status", "type": "STRING", "mode": "NULLABLE"},
                    ]
                },
            }
        }
    }

    body = (
        f"--{boundary}\r\n"
        f"Content-Type: application/json; charset=UTF-8\r\n"
        f"\r\n"
        f"{json.dumps(metadata)}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: application/octet-stream\r\n"
        f"\r\n"
        f"{ndjson}\r\n"
        f"--{boundary}--\r\n"
    ).encode("utf-8")

    url = (
        f"https://bigquery.googleapis.com/upload/bigquery/v2/"
        f"projects/{PROJECT}/jobs?uploadType=multipart"
    )
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", f"multipart/related; boundary={boundary}")

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            response = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        raise RuntimeError(f"BigQuery API error (HTTP {e.code}): {error_body}")

    job_id = response.get("jobReference", {}).get("jobId", "unknown")
    print(f"Load job submitted: {job_id}, state={response.get('status', {}).get('state')}")

    # Poll until done
    job_url = (
        f"https://bigquery.googleapis.com/bigquery/v2/"
        f"projects/{PROJECT}/jobs/{job_id}"
    )
    for _ in range(60):
        time.sleep(5)
        req2 = urllib.request.Request(job_url)
        req2.add_header("Authorization", f"Bearer {bq.access_token()}")
        with urllib.request.urlopen(req2, timeout=30) as resp:
            status = json.loads(resp.read().decode("utf-8"))
        state = status.get("status", {}).get("state", "unknown")
        if state == "DONE":
            if status["status"].get("errorResult"):
                raise RuntimeError(
                    f"Load job failed: {json.dumps(status['status']['errorResult'], indent=2)}"
                )
            output_rows = int(
                status.get("statistics", {}).get("load", {}).get("outputRows", 0)
            )
            return output_rows
        print(f"  Job state: {state}")

    raise RuntimeError("Load job timed out after 300s")


def main() -> int:
    print("=" * 60)
    print("delist_status sync: CDP → BigQuery")
    print(f"Target: {PROJECT}.{DATASET}.{TABLE}")
    print("=" * 60)

    records = fetch_from_cdp()
    if not records:
        print("ERROR: No records fetched from CDP — aborting to avoid wiping table")
        return 1

    output_rows = load_to_bq(records)
    print(f"Done! {output_rows} rows written to {TABLE}")

    # Verify
    print("Verifying...")
    sql = f"SELECT COUNT(*) AS cnt FROM `{PROJECT}.{DATASET}.{TABLE}`"
    result = bq.run_query(sql)
    count = result["rows"][0]["f"][0]["v"] if result.get("rows") else "?"
    print(f"Table now has {count} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
"""Daily load of treasury_summary from Tableau into BigQuery.

Intended to run as a Cloud Run job. Fetches the Asset/Liability partitions for
the target date, transforms the rows, uploads a CSV to GCS, loads it into
`finance_treasury.treasury_summary`, and then rebuilds `finance_treasury.deficit_tableau`.
"""
import csv
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from common import bq, secrets  # noqa: E402

PROJECT = os.environ.get("BQ_PROJECT", "treasury-datamart-sandbox")
DATASET = os.environ.get("BQ_DATASET", "finance_treasury")
LOCATION = os.environ.get("BQ_LOCATION", "us")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "treasury_summary")

TABLEAU_SERVER = os.environ.get("TABLEAU_SERVER", "https://tableau.crypt0co.com")
TABLEAU_API_VERSION = os.environ.get("TABLEAU_API_VERSION", "3.21")
TABLEAU_VIEW_ID = os.environ.get("TABLEAU_VIEW_ID", "bc53e13f-33b5-4509-b0a7-14c9a5843059")
TABLEAU_SITE_URL = os.environ.get("TABLEAU_SITE_URL", "CryptocomFDT")

COLUMN_MAPPING = {
    "GROUPING": "ASSET_LIABILITY",
    "TYPE": "BALANCE_SHEET_ITEM",
    "NAME": "TYPE",
    "booking_entity": "ENTITY",
    "Reporting Token": "CURRENCY",
    "token_group": "CURRENCY_GRP",
    "Net Value": "BAL_CCY",
    "Net USD Amount": "BAL_USD",
    "token_type": "TOKEN_TYPE",
    "is_trade_only": "IS_TRADE_ONLY",
    "delist_status": "DELIST_STATUS",
    "is_memo": "IS_MEMO",
    "is_pos_token": "IS_POS_TOKEN",
    "counterparty": "COUNTERPARTY",
    "network": "NETWORK",
    "profile": "PROFILE",
    "state_of_incorporation": "STATE_OF_INCORPORATION",
}
CURRENCY_GRP_REMOVE = {"Stable Coin", "Particle B", "Trust Token", "Fiat"}
BQ_COLUMNS = [
    "REPORT_DATE", "ASSET_LIABILITY", "BALANCE_SHEET_ITEM", "TYPE", "ENTITY",
    "CURRENCY", "CURRENCY_GRP", "BAL_CCY", "BAL_USD", "TOKEN_TYPE",
    "IS_TRADE_ONLY", "DELIST_STATUS", "IS_MEMO", "IS_POS_TOKEN",
    "COUNTERPARTY", "NETWORK", "PROFILE", "STATE_OF_INCORPORATION",
    "EXTRACTED_TIMESTAMP",
]


def tableau_auth(pat_name: str, pat_secret: str) -> tuple[str, str]:
    url = f"{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/auth/signin"
    payload = json.dumps({
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": TABLEAU_SITE_URL},
        }
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={
        "accept": "application/json", "content-type": "application/json"
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    return body["credentials"]["token"], body["credentials"]["site"]["id"]


def fetch_partition(tab_token: str, site_id: str, report_date: date, grouping: str) -> str:
    yyyymmdd = report_date.strftime("%Y%m%d")
    url = (
        f"{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/sites/{site_id}"
        f"/views/{TABLEAU_VIEW_ID}/data"
        f"?vf_execution_date={yyyymmdd}&vf_GROUPING={grouping}"
    )
    req = urllib.request.Request(url, headers={"X-Tableau-Auth": tab_token})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8")


def fetch_all_rows(tab_token: str, site_id: str, report_date: date) -> list[dict]:
    all_rows = []
    for grp in ["Asset", "Liability"]:
        print(f"    Fetching {grp} partition...")
        csv_text = fetch_partition(tab_token, site_id, report_date, grp)
        lines = [l for l in csv_text.split("\n") if l.strip()]
        if len(lines) <= 1:
            print(f"    WARNING: {grp} returned empty")
            continue
        reader = csv.DictReader(io.StringIO(csv_text.replace("\r\n", "\n").replace("\r", "\n")))
        rows = list(reader)
        print(f"    {grp}: {len(rows)} rows")
        all_rows.extend(rows)
    return all_rows


def transform_rows(rows: list[dict], report_date: date) -> list[dict]:
    extracted_ts = datetime.utcnow().isoformat()
    result = []
    for row in rows:
        bq_row = {}
        for field in BQ_COLUMNS:
            if field == "REPORT_DATE":
                bq_row[field] = report_date.isoformat()
            elif field == "EXTRACTED_TIMESTAMP":
                bq_row[field] = extracted_ts
            else:
                tab_col = next((k for k, v in COLUMN_MAPPING.items() if v == field), None)
                value = row.get(tab_col, "") if tab_col else ""

                if value == "N/A":
                    value = ""

                if field == "CURRENCY_GRP":
                    if not value or value in CURRENCY_GRP_REMOVE:
                        value = row.get("Reporting Token", "")

                if field.startswith("IS_"):
                    if value in ("True", "true", "1"):
                        value = "true"
                    elif value in ("False", "false", "0"):
                        value = "false"
                    else:
                        value = ""

                bq_row[field] = value if value is not None else ""
        result.append(bq_row)
    return result


def rows_to_csv_bytes(rows: list[dict]) -> bytes:
    out = io.StringIO()
    writer = csv.DictWriter(
        out, fieldnames=BQ_COLUMNS, lineterminator="\n",
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode("utf-8")


def upload_to_gcs(csv_bytes: bytes, file_name: str) -> str:
    t = bq.access_token()
    url = (
        f"https://storage.googleapis.com/upload/storage/v1/b/{GCS_BUCKET}/o"
        f"?uploadType=media&name={file_name}"
    )
    req = urllib.request.Request(url, data=csv_bytes, headers={
        "Authorization": f"Bearer {t}",
        "Content-Type": "text/csv",
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read())
    return body.get("name")


def bq_delete(report_date: date) -> None:
    sql = (
        f"DELETE FROM `{PROJECT}.{DATASET}.treasury_summary`"
        f" WHERE REPORT_DATE = '{report_date.isoformat()}'"
    )
    bq.run_query(sql)


def bq_load_from_gcs(uri: str) -> int:
    status = bq.run_query(
        f"""
        LOAD DATA INTO `{PROJECT}.{DATASET}.treasury_summary`
        FROM FILES (uris = ['{uri}'],
                    format = 'CSV',
                    skip_leading_rows = 1,
                    null_marker = "",
                    allow_quoted_newlines = true)
        """
    )
    # LOAD DATA does not return numDmlAffectedRows in the same field; query it.
    return 0


def rebuild_deficit_tableau() -> None:
    """Placeholder: rebuild deficit_tableau via the patched scheduled query SQL."""
    # The full SQL is large and maintained separately. For now, trigger the
    # existing BigQuery Data Transfer or run the SQL from a stored file.
    # If an env var DEFICIT_SQL_GCS points to the SQL file, run it.
    sql = os.environ.get("DEFICIT_REBUILD_SQL")
    if sql:
        bq.run_query(sql)
        print("  Rebuilt deficit_tableau")
    else:
        print("  Skipping deficit_tableau rebuild (DEFICIT_REBUILD_SQL not set)")


def upload_date(tab_token: str, site_id: str, report_date: date) -> int:
    print(f"\n  [UPLOAD {report_date.isoformat()}]")

    rows = fetch_all_rows(tab_token, site_id, report_date)
    if not rows:
        raise RuntimeError(f"no rows from Tableau for {report_date}")

    groupings = {r.get("GROUPING") for r in rows}
    if "Asset" not in groupings or "Liability" not in groupings:
        print(f"    WARNING: incomplete groupings: {groupings}")

    bq_rows = transform_rows(rows, report_date)
    print(f"    Transformed: {len(bq_rows)} rows")

    csv_bytes = rows_to_csv_bytes(bq_rows)
    print(f"    CSV size: {len(csv_bytes):,} bytes")

    file_name = f"treasury_summary_{report_date.strftime('%Y%m%d')}_{int(time.time())}.csv"
    print(f"    Uploading to GCS: {file_name}...")
    upload_to_gcs(csv_bytes, file_name)

    bq_delete(report_date)
    uri = f"gs://{GCS_BUCKET}/{file_name}"
    bq_load_from_gcs(uri)
    print(f"    Loaded {len(bq_rows)} rows from GCS to BQ")

    return len(bq_rows)


def previous_business_day(reference: date | None = None) -> date:
    d = reference or datetime.utcnow().date()
    # Move back one day, further if weekend
    d = d - timedelta(days=1)
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d = d - timedelta(days=1)
    return d


def main() -> int:
    target_date_str = os.environ.get("TARGET_DATE")
    if target_date_str:
        report_date = date.fromisoformat(target_date_str)
    else:
        report_date = previous_business_day()

    print("=" * 60)
    print(f"treasury_summary load for {report_date.isoformat()}")
    print("=" * 60)

    pat_name = secrets.get_secret("tableau-pat-name")
    pat_secret = secrets.get_secret("tableau-pat-secret")

    tab_token, site_id = tableau_auth(pat_name, pat_secret)
    print(f"[1] Tableau auth OK (site_id={site_id})")

    upload_date(tab_token, site_id, report_date)
    print(f"[2] {report_date} uploaded OK")

    rebuild_deficit_tableau()
    print("[3] Done")

    return 0


if __name__ == "__main__":
    sys.exit(main())

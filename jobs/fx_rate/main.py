"""Daily sync of finance_treasury.fx_rate from Tableau.

Intended to run as a Cloud Run job. Fetches FX rates for the target date from the
Tableau ``fx_rate`` workbook, deletes any existing rows for that date in
BigQuery, and inserts the fresh rows.
"""
import csv
import io
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from common import bq, secrets  # noqa: E402
from common.tableau import fetch_view_data, sign_in  # noqa: E402

BQ_PROJECT = os.environ.get("BQ_PROJECT", "treasury-datamart-sandbox")
BQ_DATASET = os.environ.get("BQ_DATASET", "finance_treasury")
TABLEAU_VIEW_ID = os.environ.get("TABLEAU_VIEW_ID", "9f86a882-b25a-462a-a721-9e6ea67fec6d")


def parse_rate(value: str) -> float | None:
    """Parse a Tableau measure value into a float, returning None when empty."""
    if value is None:
        return None
    cleaned = value.strip().replace(",", "")
    if cleaned in ("", "null", "NULL", "N/A"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def fetch_tableau_fx(target_date: date, pat_name: str, pat_secret: str) -> list[tuple[str, float]]:
    """Return list of (symbol, exchange_rate) for target_date from Tableau."""
    tab_token, site_id = sign_in(pat_name, pat_secret)
    # Tableau accepts ISO date strings for the "Report Date" filter on this view.
    date_param = target_date.strftime("%Y-%m-%d")
    csv_text = fetch_view_data(
        TABLEAU_VIEW_ID,
        tab_token,
        site_id,
        params={"vf_Report%20Date": date_param},
    )

    reader = csv.DictReader(io.StringIO(csv_text.replace("\r\n", "\n").replace("\r", "\n")))
    rows: list[tuple[str, float]] = []
    for row in reader:
        symbol = row.get("Reporting Currency", "").strip()
        value = parse_rate(row.get("Measure Values", ""))
        if not symbol or value is None:
            continue
        rows.append((symbol, value))
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

    pat_name = secrets.get_secret("tableau-pat-name")
    pat_secret = secrets.get_secret("tableau-pat-secret")

    rows = fetch_tableau_fx(target_date, pat_name, pat_secret)
    if not rows:
        print(f"  {d_str}: no data from Tableau, nothing to sync")
        return 0

    print(f"  {d_str}: fetched {len(rows)} symbols from Tableau")

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

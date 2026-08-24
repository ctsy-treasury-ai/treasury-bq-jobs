"""Lightweight BigQuery helper using the REST API + Application Default Credentials."""
import os
import subprocess
import time
from typing import Any

import requests

_PROJECT = os.environ.get("BQ_PROJECT", "treasury-datamart-sandbox")


def access_token() -> str:
    """Return a valid GCP access token, preferring ADC in Cloud Run."""
    try:
        return subprocess.check_output(
            ["gcloud", "auth", "print-access-token"], text=True
        ).strip()
    except Exception:  # pragma: no cover
        import google.auth.transport.requests
        from google.auth import default
        creds, _ = default()
        creds.refresh(google.auth.transport.requests.Request())
        return creds.token


def _token() -> str:
    return access_token()


def run_query(sql: str, wait: bool = True, project: str = _PROJECT) -> dict[str, Any]:
    """Submit a BigQuery query job and optionally wait for completion."""
    token = _token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs",
        headers=headers,
        json={"configuration": {"query": {"query": sql, "useLegacySql": False}}},
    )
    resp.raise_for_status()
    job = resp.json()
    job_id = job["jobReference"]["jobId"]

    if not wait:
        return job

    for i in range(120):
        time.sleep(5)
        status_resp = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {_token()}"},
        )
        status_resp.raise_for_status()
        status = status_resp.json()
        state = status["status"]["state"]
        if state == "DONE":
            if status["status"].get("errorResult"):
                raise RuntimeError(f"BQ job failed: {status['status']['errorResult']}")
            return status
        if (i + 1) % 6 == 0:
            print(f"  Still running... ({(i+1)*5}s)")
    raise RuntimeError("BQ job timed out after 600s")


def query_once(sql: str, project: str = _PROJECT) -> dict[str, Any]:
    """Run a query via the queries endpoint (no job polling)."""
    token = _token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries",
        headers=headers,
        json={"query": sql, "useLegacySql": False, "timeoutMs": 60000},
    )
    resp.raise_for_status()
    return resp.json()


def load_table_from_gcs(
    uri: str,
    table_ref: str,
    *,
    project: str = _PROJECT,
    skip_leading_rows: int = 1,
    null_marker: str = "",
    allow_quoted_newlines: bool = True,
    autodetect: bool = False,
    write_disposition: str = "WRITE_APPEND",
) -> dict[str, Any]:
    """Load a CSV from GCS into an existing BigQuery table using the jobs API.

    Provides explicit control over schema autodetection, avoiding the SQL
    ``LOAD DATA`` statement's default inference behavior.
    """
    parts = table_ref.split(".")
    if len(parts) != 3:
        raise ValueError("table_ref must be in project.dataset.table format")

    token = _token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "configuration": {
            "load": {
                "sourceUris": [uri],
                "destinationTable": {
                    "projectId": parts[0],
                    "datasetId": parts[1],
                    "tableId": parts[2],
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": skip_leading_rows,
                "nullMarker": null_marker,
                "allowQuotedNewlines": allow_quoted_newlines,
                "autodetect": autodetect,
                "writeDisposition": write_disposition,
            }
        }
    }
    resp = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs",
        headers=headers,
        json=body,
    )
    resp.raise_for_status()
    job = resp.json()
    job_id = job["jobReference"]["jobId"]

    for i in range(120):
        time.sleep(5)
        status_resp = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/{job_id}",
            headers={"Authorization": f"Bearer {_token()}"},
        )
        status_resp.raise_for_status()
        status = status_resp.json()
        state = status["status"]["state"]
        if state == "DONE":
            if status["status"].get("errorResult"):
                raise RuntimeError(f"BQ load job failed: {status['status']['errorResult']}")
            return status
        if (i + 1) % 6 == 0:
            print(f"  Still running load job... ({(i+1)*5}s)")
    raise RuntimeError("BQ load job timed out after 600s")

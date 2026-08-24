"""Minimal Tableau REST API helpers used by Cloud Run jobs."""
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

TABLEAU_SERVER = os.environ.get("TABLEAU_SERVER", "https://tableau.crypt0co.com")
TABLEAU_API_VERSION = os.environ.get("TABLEAU_API_VERSION", "3.21")
TABLEAU_SITE_URL = os.environ.get("TABLEAU_SITE_URL", "CryptocomFDT")


def sign_in(pat_name: str, pat_secret: str) -> tuple[str, str]:
    """Authenticate to Tableau and return (auth_token, site_id)."""
    url = f"{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/auth/signin"
    payload = json.dumps(
        {
            "credentials": {
                "personalAccessTokenName": pat_name,
                "personalAccessTokenSecret": pat_secret,
                "site": {"contentUrl": TABLEAU_SITE_URL},
            }
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"accept": "application/json", "content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    return body["credentials"]["token"], body["credentials"]["site"]["id"]


def fetch_view_data(view_id: str, tab_token: str, site_id: str, params: Optional[dict] = None, timeout: int = 120) -> str:
    """Fetch CSV data for a view. ``params`` are URL query parameters."""
    query = ""
    if params:
        query_parts = []
        for key, value in params.items():
            encoded = urllib.parse.quote(str(value), safe="")
            query_parts.append(f"{key}={encoded}")
        query = "&".join(query_parts)
    url = f"{TABLEAU_SERVER}/api/{TABLEAU_API_VERSION}/sites/{site_id}/views/{view_id}/data"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(url, headers={"X-Tableau-Auth": tab_token})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Tableau API error ({exc.code}): {exc.reason}") from exc

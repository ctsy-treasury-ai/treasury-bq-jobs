"""Helpers for reading secrets from environment or GCP Secret Manager.

Cloud Run: mount the secret as an env var (e.g. IMPALA_PASSWORD) and the
helper will return it directly.

Local dev: set the env var with the same name, or set `<NAME>_SECRET`.
"""
import base64
import os
import subprocess

import requests


_GCP_TOKEN = None


def _access_token() -> str:
    global _GCP_TOKEN
    if _GCP_TOKEN is None:
        try:
            _GCP_TOKEN = subprocess.check_output(
                ["gcloud", "auth", "print-access-token"], text=True
            ).strip()
        except Exception:  # pragma: no cover
            import google.auth.transport.requests
            from google.auth import default
            creds, _ = default()
            creds.refresh(google.auth.transport.requests.Request())
            _GCP_TOKEN = creds.token
    return _GCP_TOKEN


def get_secret(name: str, project: str = "treasury-datamart-sandbox") -> str:
    """Return the value of a secret.

    Resolution order:
    1. Env var with the exact ``name`` (Cloud Run --set-secrets style).
    2. Env var ``<NAME>_SECRET`` (local development override).
    3. GCP Secret Manager latest version.
    """
    env_key = name.replace("-", "_").upper()
    override_key = f"{env_key}_SECRET"

    if env_key in os.environ:
        return os.environ[env_key]
    if override_key in os.environ:
        return os.environ[override_key]

    token = _access_token()
    url = (
        f"https://secretmanager.googleapis.com/v1/projects/{project}/"
        f"secrets/{name}/versions/latest:access"
    )
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    return base64.b64decode(resp.json()["payload"]["data"]).decode("utf-8")

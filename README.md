# Treasury BigQuery Cloud Run Jobs

Cloud Run Jobs that load daily data into `finance_treasury` BigQuery tables.

## Jobs

- **`jobs/fx_rate/main.py`**: Syncs FX rates from CDP Impala `fx_rates_dt` to `finance_treasury.fx_rate`.
- **`jobs/treasury_summary/main.py`**: Loads `treasury_summary` from a Tableau view and rebuilds `deficit_tableau`.

## Local development

Set env vars:

```bash
export IMPALA_PASSWORD_SECRET="..."
export TABLEAU_PAT_NAME_SECRET="..."
export TABLEAU_PAT_SECRET_SECRET="..."
```

Run a job:

```bash
python -m jobs.fx_rate.main
# or
TARGET_DATE=2026-08-20 python -m jobs.treasury_summary.main
```

## Deployment

Pushes to `main` trigger `.github/workflows/deploy.yml`, which:

1. Builds a Docker image and pushes it to Artifact Registry.
2. Deploys/updates two Cloud Run Jobs.
3. Configures Cloud Scheduler to run each job daily.

Required GitHub secrets:
- `GCP_SA_KEY`: service account JSON key for `treasury-bq-jobs@treasury-datamart-sandbox.iam.gserviceaccount.com`.

Required GCP secrets (Secret Manager):
- `impala-password`
- `tableau-pat-name`
- `tableau-pat-secret`

## Schedules

- FX rate sync: daily at 08:30 UTC
- Treasury summary load: daily at 10:00 UTC

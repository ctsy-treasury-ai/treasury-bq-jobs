FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY jobs/ ./jobs/

# Default module can be overridden per-job at deploy time
ENTRYPOINT ["python", "-m"]
CMD ["jobs.fx_rate.main"]

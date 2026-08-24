FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for impyla/sasl
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-dev \
    libkrb5-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY jobs/ ./jobs/

# Default module can be overridden per-job at deploy time
ENTRYPOINT ["python", "-m"]
CMD ["jobs.fx_rate.main"]

FROM python:3.10-slim

WORKDIR /ge

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    great_expectations==1.6.3 \
    psycopg2-binary \
    "sqlalchemy<2.0"

CMD ["sleep", "infinity"]


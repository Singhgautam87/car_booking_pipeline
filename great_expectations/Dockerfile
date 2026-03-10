FROM python:3.10-slim
WORKDIR /ge

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

# ✅ Great Expectations + DB connectors
RUN pip install --no-cache-dir \
    great_expectations==1.6.3 \
    psycopg2-binary \
    "sqlalchemy<2.0"

# ✅ Kafka Producer + Validation dependencies — pehle se install
# Jenkinsfile mein runtime pip install nahi karna padega
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    mysql-connector-python \
    psycopg2-binary

CMD ["tail", "-f", "/dev/null"]
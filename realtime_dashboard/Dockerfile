FROM python:3.10-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# ✅ All packages pre-installed — no runtime pip install
RUN pip install --no-cache-dir \
    dash==2.14.2 \
    plotly==5.18.0 \
    pandas==2.1.4 \
    sqlalchemy==2.0.23 \
    psycopg2-binary==2.9.9 \
    mysql-connector-python==8.2.0 \
    numpy==1.26.2

COPY dashboard.py /app/dashboard.py

EXPOSE 8050
CMD ["python", "/app/dashboard.py"]
FROM python:3.10-slim

WORKDIR /app

# ✅ Sab packages pehle se install — no runtime pip install needed
RUN pip install --no-cache-dir \
    dash==2.14.2 \
    plotly==5.18.0 \
    pandas==2.1.4 \
    sqlalchemy==2.0.23 \
    psycopg2-binary==2.9.9 \
    mysql-connector-python==8.2.0 \
    numpy==1.26.2

# ✅ Dashboard file copy
COPY dashboard.py /app/dashboard.py

EXPOSE 8050

CMD ["python", "/app/dashboard.py"]
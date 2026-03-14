FROM python:3.12-slim

WORKDIR /app

COPY app.py /app/app.py

ENV PYTHONUNBUFFERED=1
ENV LOG_FILE=/var/log/market/market-python.ndjson
ENV LOGS_PER_MINUTE=100
ENV SERVICE_NAME=market-simulator
ENV HOST_NAME=market-python-log-producer
ENV ECS_VERSION=9.3.0

CMD ["python", "/app/app.py"]

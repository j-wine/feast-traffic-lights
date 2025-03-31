FROM python:3.10.11

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    libffi-dev \
    python3-dev \
    python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip


WORKDIR /app
COPY requirements.txt /app/requirements.txt
COPY kafka_producer.py /app/kafka_producer.py
COPY feature_store.yaml /app/feature_store.yaml
# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt
# wait for kafka broker
CMD ["sh", "-c", "sleep 10 && python kafka_producer.py"]

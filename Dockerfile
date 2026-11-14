# The consumer, the workload driver and the connector registration all run from this image.
FROM python:3.11-slim-bookworm

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt requirements-stack.txt ./
RUN pip install --no-cache-dir -r requirements-stack.txt

COPY config ./config
COPY docker ./docker
COPY src ./src

VOLUME ["/app/data"]
ENTRYPOINT ["python", "-m", "cdc_lake"]
CMD ["--help"]

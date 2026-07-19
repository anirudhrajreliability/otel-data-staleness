# SDK-in-the-live-path service image.
#   Build context is the REPO ROOT so it can COPY the python/ package.
#   docker build -f deploy/integration/sdk-service/Dockerfile -t sdk-service .
FROM python:3.12-slim
WORKDIR /app
COPY python/ /app/otel-staleness/
COPY deploy/integration/sdk-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
 && pip install --no-cache-dir /app/otel-staleness
COPY deploy/integration/sdk-service/sdk_probe_service.py /app/sdk_probe_service.py
CMD ["python", "-u", "/app/sdk_probe_service.py"]

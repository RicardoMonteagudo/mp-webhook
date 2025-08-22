# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run expone $PORT
ENV PORT=8080
CMD exec gunicorn --bind :$PORT --workers 2 --threads 8 --timeout 0 app:app

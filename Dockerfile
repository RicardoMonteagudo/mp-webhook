# Dockerfile
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run te inyecta $PORT; por si corres local, déjalo en 8080
ENV PORT=8080

# Logs verbosos para ver cualquier error de import en gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:${PORT}", "--workers", "2", "--threads", "8", "--log-level", "debug", "--capture-output", "app:app"]

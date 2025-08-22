FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Gunicorn lee PORT del entorno (Cloud Run lo pone)
CMD ["bash", "-lc", "gunicorn -b 0.0.0.0:${PORT:-8080} -w 2 --threads 4 --timeout 30 app:app"]

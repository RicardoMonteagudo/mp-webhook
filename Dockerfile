FROM python:3.10-slim

# Buenas prácticas de Python/PIP
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tu código
COPY . .

# Corre como usuario no-root (más seguro)
RUN useradd -m appuser
USER appuser

# Cloud Run inyecta $PORT; por defecto usa 8080
ENV PORT=8080

# Gunicorn con:
# - 2 workers + 4 threads (ligero y suficiente para webhooks)
# - timeouts bajos (webhook debe responder rápido)
# - poco ruido en logs (warning+error; sin access log)
# - reciclaje periódico de workers (estabilidad)
CMD ["bash", "-lc", "exec gunicorn app:app \
  -b 0.0.0.0:${PORT} \
  -w 2 --threads 4 \
  --worker-class gthread \
  --timeout 25 --graceful-timeout 10 --keep-alive 5 \
  --max-requests 1000 --max-requests-jitter 100 \
  --log-level warning --error-logfile - --access-logfile /dev/null"]

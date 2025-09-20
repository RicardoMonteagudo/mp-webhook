# Imagen base
FROM python:3.12-slim

# Paquetes nativos para psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código
COPY . .

# Directorio para el socket de Cloud SQL
RUN mkdir -p /cloudsql

# Vars básicas
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Gunicorn (archivo se llama app.py → app:app)]
CMD ["sh","-c","gunicorn -w 2 -k gthread --threads 8 -t 60 --access-logfile - --error-logfile - -b :${PORT:-8080} app:app"]


# Imagen base liviana con Python 3.12
FROM python:3.12-slim

# Instalar dependencias necesarias para psycopg2
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Directorio requerido para el socket de Cloud SQL
RUN mkdir -p /cloudsql

# Puerto (Cloud Run lo expone con $PORT)
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Arranque con gunicorn
CMD ["sh","-c","gunicorn -b :${PORT:-8080} app:app"]

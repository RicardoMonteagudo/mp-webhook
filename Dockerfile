# Imagen base con Python
FROM python:3.10

# Crear directorio de trabajo
WORKDIR /app

# Copiar e instalar dependencias
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copiar el resto del código
COPY . .

# Exponer el puerto 8080 (Cloud Run lo usa por defecto)
EXPOSE 8080

# Usar gunicorn como servidor de producción
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]

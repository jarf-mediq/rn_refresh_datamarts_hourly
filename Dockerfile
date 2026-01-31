# Dockerfile para ETL Incremental
FROM python:3.12-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivo de dependencias
COPY requirements.txt .

# Instalar dependencias del sistema necesarias para psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY src/ /app/src/

# Establecer el directorio de trabajo para la ejecución
WORKDIR /app

# Comando por defecto: ejecutar el ETL incremental
CMD ["python", "src/etl_incremental.py"]
FROM python:3.11-slim

# Instala Java (necesario para Spark) y herramientas del sistema
RUN apt-get update && apt-get install -y \
    default-jre \
    procps \
    && apt-get clean

# Configura las rutas para que el sistema encuentre Spark
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /app

# Instala los ingredientes que pusiste en el requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia tu código y carpetas al contenedor
COPY . .

# Indica que el archivo principal a ejecutar es main.py [cite: 159]
CMD ["python", "src/main.py"]

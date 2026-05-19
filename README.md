# End-to-End Financial Data Lakehouse Pipeline (Apache Iceberg & PySpark)

Este proyecto implementa una arquitectura de **Data Lakehouse** de nivel empresarial para la ingesta, procesamiento y modelado dimensional de tipos de cambio financieros (basado en la API de Frankfurter) y transacciones bancarias. 

El sistema está diseñado de forma modular, implementando captura de cambios en datos (**CDC**), cálculo de métricas avanzadas mediante Window Functions, detección de anomalías y un **Modelado Dimensional (Star Schema)** analítico.

---

## Guía de Despliegue y Ejecución (Quick Start)

Para levantar el entorno desde cero, construir la imagen con los últimos cambios de código, ejecutar el pipeline completo y validar la integridad con la suite de pruebas, ejecuta los siguientes comandos en tu terminal:

# 1. Navegar al directorio del proyecto
cd ~/konfio_project

# 2. Construir y asegurar la última versión de la imagen Docker
docker compose build

# 3. Ejecutar el Pipeline e Ingesta completa de extremo a extremo
docker compose run --rm spark-iceberg-pipeline python src/main.py

# 4. Correr la suite de pruebas unitarias y de integración de forma detallada
docker compose run --rm spark-iceberg-pipeline pytest -v -s
🌦️ Climate Data Pipeline with Apache Airflow

Este proyecto implementa un pipeline de datos automatizado para la extracción, transformación y carga (ETL) de información climática desde Weather Underground, utilizando Apache Airflow, PostgreSQL y Docker.

El sistema permite recolectar datos históricos y actuales de múltiples estaciones meteorológicas y almacenarlos de forma estructurada para su posterior análisis.

📌 Arquitectura del Proyecto

El pipeline sigue una arquitectura ETL orquestada con Airflow:

Extract

Web scraping de datos climáticos desde Weather Underground

Múltiples estaciones meteorológicas

Manejo de fechas y rangos históricos

Transform

Limpieza de datos

Conversión de direcciones de viento a ángulos

Normalización de valores

Estructuración en DataFrames con Pandas

Load

Inserción de datos en PostgreSQL

Persistencia de datos históricos y actuales

🧰 Tecnologías Utilizadas

Python 3.9

Apache Airflow 2.9.1

PostgreSQL 15

Docker & Docker Compose

Pandas

BeautifulSoup

Requests

SQLAlchemy

📂 Estructura del Proyecto
.
├── dags/
│   └── DAG.py              # DAG de Airflow (ETL automatizado)
├── CARGA.py                # Script de carga histórica de datos
├── docker-compose.yml      # Infraestructura Docker (Airflow + PostgreSQL)
├── init-db.sh              # Script de inicialización de base de datos
└── README.md

⚙️ Descripción de Archivos
🔁 DAG.py

Define un DAG de Airflow

Ejecuta el proceso ETL de forma programada

Extrae datos climáticos diarios

Transforma variables como dirección del viento

Carga los datos en PostgreSQL

Incluye:

Reintentos automáticos

Manejo de errores

Control de fechas

📦 CARGA.py

Script independiente para carga histórica

Recorre un rango de fechas (2023–2025)

Extrae datos por estación y por día

Ideal para poblar la base de datos inicial

🐳 docker-compose.yml

Levanta toda la infraestructura necesaria:

PostgreSQL

Airflow Webserver

Airflow Scheduler

Incluye:

Persistencia de datos con volúmenes

Zona horaria configurada (America/Merida)

Variables de entorno seguras

Puertos expuestos:

PostgreSQL: 5432

Airflow UI: 8080

🚀 Instalación y Ejecución
1️⃣ Clonar el repositorio
git clone https://github.com/tu-usuario/tu-repo.git
cd tu-repo

2️⃣ Levantar los servicios con Docker
docker-compose up -d

3️⃣ Acceder a Airflow

URL: http://localhost:8080

Usuario y contraseña por defecto (según configuración de Airflow)

🧪 Ejecución del Pipeline

Activa el DAG desde la interfaz de Airflow

El scheduler ejecutará automáticamente el proceso ETL

Los datos se almacenan en PostgreSQL

🗄️ Base de Datos

Motor: PostgreSQL

Base de datos: prueba_pipeline

Contiene datos climáticos por:

Estación

Fecha

Variables meteorológicas

Dirección del viento convertida a ángulo

🎯 Objetivo del Proyecto

Este proyecto tiene como objetivo:

Aplicar conceptos de Data Engineering

Diseñar un pipeline reproducible y escalable

Integrar orquestación de datos, scraping, y almacenamiento

Servir como base para análisis climático o modelos predictivos

📌 Posibles Mejoras

Implementar validación de datos (Great Expectations)

Agregar visualización con dashboards

Migrar a un entorno distribuido

Añadir alertas y monitoreo

👨‍💻 Autor

Alejandro Caballero
Proyecto académico / Data Engineering

# TecnoMundo ETL Pipeline & Databricks Lakehouse

Este repositorio contiene un pipeline de datos ETL (Extracción, Transformación y Carga) completo y automatizado, diseñado para procesar y analizar los datos de ventas de TecnoMundo. El proyecto implementa una arquitectura Medallion en Databricks para asegurar la calidad, fiabilidad y escalabilidad de los datos.

El pipeline ingesta datos crudos de ventas, los limpia, los enriquece con un maestro de productos y los prepara en una tabla final (Capa de Oro) lista para el consumo por parte de analistas de negocio, científicos de datos o aplicaciones externas como una API.

---

### Índice
* [Arquitectura del Pipeline](#arquitectura-del-pipeline)
* [Estructura del Repositorio](#estructura-del-repositorio)
* [Guía del Desarrollador y Flujo de Trabajo](#guía-del-desarrollador-y-flujo-de-trabajo)
  * [Requisitos Previos](#requisitos-previos)
  * [Instalación](#instalación)
  * [Ciclo de Desarrollo y Ejecución](#ciclo-de-desarrollo-y-ejecución)
* [Autor](#autor)

---

## Arquitectura del Pipeline

El pipeline sigue una **Arquitectura Medallion** de tres capas, un estándar en la industria para la construcción de Lakehouses de datos:

### 1. Capa de Bronce (Bronze Layer)
* **Propósito:** Ingesta de datos crudos.
* **Tablas:** `..._raw`
* **Descripción:** Esta capa contiene los datos tal como llegan de los sistemas de origen. Es una copia fiel y sin procesar, crucial para la auditoría y el reprocesamiento. Se realizan saneamientos técnicos mínimos (ej. en nombres de columnas) para permitir el almacenamiento.

### 2. Capa de Plata (Silver Layer)
* **Propósito:** Limpieza y validación de datos.
* **Tablas:** `..._cleaned`
* **Descripción:** Los datos de la Capa de Bronce se limpian, se validan los tipos de datos, se manejan los valores nulos y se aplican reglas de modelado (como la estandarización de claves y la eliminación de columnas redundantes). El resultado es un conjunto de datos fiable y consistente.

### 3. Capa de Oro (Gold Layer)
* **Propósito:** Enriquecimiento y lógica de negocio.
* **Tablas:** `..._categorized`
* **Descripción:** Los datos limpios de la Capa de Plata se enriquecen con el contexto de negocio. En este caso, se unen con la tabla de dimensiones de categorías para añadir el nombre y la categoría a cada venta. El resultado es una tabla final, lista para el consumo.


---

## Estructura del Repositorio

El proyecto está organizado de manera optima y clara para facilitar el desarrollo y la colaboración. A continuación se detalla la estructura de carpetas:

```text
├── `data/`               Datos locales
│   ├── `raw/`            Archivos de datos crudos (ej. `Category.xlsx`)
│   └── `updates/`        Archivos de novedades para actualizar dimensiones
├── `notebooks/`          Notebooks del proyecto
│   ├── `databricks/`     Notebooks que se ejecutan en Databricks
│   │   ├── `production/`
│   │   │   ├── `1_ingestion/`
│   │   │   ├── `2_transformation/`
│   │   │   └── `3_business_logic/`
│   │   └── `maintenance/`
│   └── `jupyters/`       Notebooks para análisis local
│       └── `data_quality_analysis/`
├── `src/`                Código fuente modular del pipeline
│   └── `tecno_etl/`
│       ├── `extractors/`
│       ├── `transformers/`
│       ├── `loaders/`
│       ├── `pipelines/`
│       └── `utils/`
└── `...`
```

## Guía del Desarrollador y Flujo de Trabajo

### Requisitos Previos
1.  Python 3.9+
2.  Tener la [CLI de Databricks (v0.205+)](https://docs.databricks.com/en/dev-tools/cli/index.html) instalada y configurada.
3.  Un archivo `.env` en la carpeta `conf/env/` con las credenciales de Databricks (`DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_TOKEN`, etc.).

### Instalación
1.  Clona el repositorio: `git clone <url_del_repositorio>`
2.  Navega a la carpeta del proyecto: `cd TecnoMundo_ETL_pipeline`
3.  Instala las dependencias: `pip install -r requirements.txt`

### Ciclo de Desarrollo y Ejecución

El pipeline se ejecuta en varias etapas orquestadas que combinan procesos locales y en la nube.

**1. Ingesta de Dimensiones (Local):**
* **Propósito:** Crear y estandarizar la tabla de verdad `category` en Databricks.
* **Acción:** El script `run_local_ingestion.py` lee el archivo `data/raw/Category.xlsx`, utiliza el módulo `data_normalizer.py` para limpiar y estandarizar las columnas, y finalmente usa el `databricks_table_loader.py` para cargar los datos en el schema `tecnomundo_data_dimensions`.
* **Ejecución:**
    ```bash
    python src/tecno_etl/pipelines/run_local_ingestion.py
    ```

**2. Ingesta de Hechos (Local -> Databricks):**
* **Propósito:** Subir un nuevo archivo de ventas (potencialmente grande) al "Staging Area" en Databricks.
* **Acción:** Se utiliza la CLI de Databricks para copiar el archivo CSV desde una ruta local al "Volume" `uploads_raw`.
* **Ejecución:**
    ```bash
    databricks fs cp "ruta/local/al/archivo_de_ventas.csv" "dbfs:/Volumes/workspace/tecnomundo_data_raw/uploads_raw/"
    ```

**3. Orquestación del Pipeline Principal (Databricks):**
* **Propósito:** Procesar el archivo de ventas subido a través de las capas Medallion.
* **Acción:** Un **Job** de Databricks llamado `Pipeline Principal de Ventas` está configurado con un **"File Arrival Trigger"**. Este disparador detecta la llegada del nuevo archivo en `uploads_raw` e inicia una secuencia de notebooks:
    1.  **`1_1_ingest_raw_sales.ipynb`**: Lee el CSV del "Volume", sanea los nombres de las columnas y crea la tabla de la **Capa de Bronce**.
    2.  **`2_1_clean_and_transform_sales.ipynb`**: Lee la tabla de Bronce, aplica la limpieza de tipos de datos, estandariza las claves y elimina columnas redundantes para crear la tabla de la **Capa de Plata**.
    3.  **`3_1_categorize_sales.ipynb`**: Lee la tabla de Plata y la tabla de dimensiones, las une para enriquecer los datos y crea la tabla final de la **Capa de Oro**.

**4. Ciclo de Mantenimiento y Calidad de Datos:**
* **Propósito:** Gestionar los productos que no se pudieron categorizar y actualizar la tabla de verdad.
* **Acción:**
    1.  **Análisis Local:** Se ejecuta el notebook `notebooks/jupyters/data_quality_analysis/DQ_unmatched_products_analysis.ipynb`. Este se conecta a la tabla de reporte en Databricks y descarga la lista de productos no encontrados.
    2.  **Corrección Manual:** Basado en el análisis, se actualiza el archivo `data/raw/Category.xlsx` o se crea un nuevo archivo de "novedades" en `data/updates/dimensions/`.
    3.  **Actualización Incremental (Local):** Se ejecuta el script `run_dimension_update.py`, que pre-procesa el archivo de novedades y sube un CSV a `uploads_dimensions`.
    4.  **Fusión en Databricks:** Un segundo Job, `Mantenimiento - Actualizar Dimensión de Categorías`, se dispara al detectar el archivo de actualización y ejecuta el notebook `update_dim_category.ipynb`, que realiza una operación `MERGE` para actualizar la tabla de verdad sin recargarla por completo.

---

## Autor

<p align="center">
  <a href="https://www.linkedin.com/in/franco-aguilera-5b3a8b1b3/">
    <img src="https://media.licdn.com/dms/image/v2/D4D03AQE1TSjtN5JVdA/profile-displayphoto-shrink_200_200/profile-displayphoto-shrink_200_200/0/1692654837750?e=1756339200&v=beta&t=m4BB0EFyn6SO5hEXcJAYsYCnaXlHTrUS4Q97fJOALHs" width="100px;" alt="Franco Aguilera"/>
  </a>
  <br/>
  <sub><b>Franco Aguilera</b></sub>
  <br/>
  <a href="https://www.linkedin.com/in/franco-aguilera-5b3a8b1b3/" title="LinkedIn">
    <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn"/>
  </a>
</p>
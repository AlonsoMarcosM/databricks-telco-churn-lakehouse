# Lakehouse y MLOps de churn telco

> **Documentación técnica:** https://alonsomarcosm.github.io/databricks-telco-churn-lakehouse/
>
> **Caso de estudio:** https://alonsomarcosm.github.io/es/projects/telco-churn-mlops-databricks/

Proyecto académico aplicado de **Big Data, Data Engineering y MLOps** desarrollado
sobre **Databricks Lakehouse** para predecir la fuga de clientes en una compañía
de telecomunicaciones a partir de datos sintéticos.

La autoría es compartida por Alonso Marcos Muñoz y Jose Barros. Alonso participa
como coautor end-to-end, con trabajo verificable en el pipeline Medallion, el
experimento MLflow, el job de ML de tres tareas y la ejecución de simulación.

El objetivo no es únicamente entrenar un modelo de machine learning, sino construir un flujo completo de datos y ML: generación de datos, ingesta, arquitectura Medallion, calidad del dato, feature engineering, entrenamiento, registro de modelo, inferencia batch, enriquecimiento con etiquetas reales, monitorización y alertas.

> **Estado del ciclo:** cualquier referencia a producción describe una simulación
> académica. No representa un sistema empresarial activo ni datos reales de clientes.

---

## Resumen ejecutivo

Este proyecto resuelve un caso de **churn prediction** en telecomunicaciones: identificar clientes con alta probabilidad de baja para priorizar campañas de retención.

La solución implementa una arquitectura end-to-end sobre Databricks:

1. Generación de datos sintéticos realistas de clientes, uso, interacciones y etiquetas.
2. Ingesta incremental con Auto Loader.
3. Pipeline Medallion: Bronze, Silver y Gold.
4. Reglas de calidad y cuarentena de datos.
5. Histórico de clientes con SCD2 / AUTO CDC.
6. Construcción de features temporales y tabla spine cliente-mes.
7. Entrenamiento distribuido con Spark MLlib.
8. Trazabilidad experimental con MLflow.
9. Registro del modelo en Unity Catalog.
10. Inferencia batch con el modelo `champion`.
11. Enriquecimiento posterior con etiquetas reales.
12. Monitorización de rendimiento, drift y alertas operativas.

---

## Problema de negocio

Una operadora telco necesita reducir la pérdida de clientes en un mercado competitivo. El sistema actual de retención no permite priorizar correctamente qué clientes tienen mayor riesgo de abandonar la compañía.

El proyecto transforma este problema en una solución analítica capaz de responder a la pregunta:

> ¿Qué clientes presentan mayor probabilidad de churn en cada periodo mensual?

La unidad de predicción es el par `cliente-mes`, lo que permite actuar comercialmente sobre clientes concretos en ventanas temporales concretas.

---

## Arquitectura lógica

### Stack tecnológico

| Área | Tecnología |
|---|---|
| Plataforma | Databricks |
| Gobierno del dato | Unity Catalog |
| Procesamiento distribuido | Apache Spark / PySpark |
| Almacenamiento transaccional | Delta Lake |
| Arquitectura de datos | Medallion Architecture |
| Ingesta incremental | Auto Loader / cloudFiles |
| Orquestación | Databricks Jobs & Lakeflow Declarative Pipelines |
| Infraestructura como código | Databricks Asset Bundles |
| Machine Learning | Spark MLlib |
| Experimentación | MLflow |
| Registro de modelos | Unity Catalog Model Registry |
| Monitorización | Lakehouse Monitoring |
| Control de versiones | Git / GitHub |
| Documentación | Markdown / LaTeX |

---

## Qué demuestra este proyecto

Este repositorio demuestra competencias prácticas en:

- diseño de arquitecturas Big Data;
- implementación de pipelines ETL/ELT distribuidos;
- modelado de datos por capas Bronze, Silver y Gold;
- gestión de calidad de datos;
- diseño de tablas de cuarentena;
- procesamiento incremental;
- feature engineering temporal;
- prevención de data leakage mediante point-in-time joins;
- entrenamiento ML distribuido;
- trazabilidad de experimentos con MLflow;
- gobierno de modelos con aliases `champion`, `challenger` y `rejected`;
- despliegue reproducible con Databricks Asset Bundles;
- inferencia batch productiva;
- monitorización de drift, rendimiento y completitud;
- separación entre training y serving;
- documentación técnica incremental.

---

## Estructura del repositorio

```text
.
├── codigo/
│   ├── databricks.yml
│   ├── resources/
│   │   ├── telco_churn.pipeline.yml
│   │   ├── telco_churn_orchestration.job.yml
│   │   ├── telco_churn_ml_orchestration.job.yml
│   │   └── telco_churn_simulation.job.yml
│   ├── notebooks/
│   │   ├── 04_Feature_Store_Registration.py
│   │   ├── 05_Training_Dataset_Generation.ipynb
│   │   ├── 07_MLflow_Experimentation.ipynb
│   │   ├── 08_Production.ipynb
│   │   ├── 09_Inference_And_Label_Enrichment.ipynb
│   │   └── 10_Simulation.ipynb
│   └── src/
│       └── medallion_pipeline/
│           ├── transformations/
│           │   ├── 01_bronze_ingestion.py
│           │   ├── 02_silver_transformation.py
│           │   ├── 03_gold_churn_spine.py
│           │   ├── 03_gold_customer_profile.py
│           │   └── 03_gold_customer_aggregations.py
│           ├── rules/
│           │   └── customers.py
│           └── utilities/
│               └── generate.py
├── documentacion/
│   ├── assets/
│   │   ├── hito2_pipeline_medallion_dag.png
│   │   ├── hito3_mlflow_experiment_aucpr.png
│   │   ├── hito3_unity_catalog_model_aliases.png
│   │   ├── hito3_job_ml_success.png
│   │   ├── hito4_simulation_job_success.png
│   │   ├── hito4_pipeline_propietario_updates.png
│   │   ├── hito4_job_diario_run_success.png
│   │   ├── hito4_inference_table_counts.png
│   │   ├── hito4_lakehouse_monitor_dashboard.png
│   │   └── hito4_alerts_ok.png
│   ├── memoria.md
│   ├── memoria.pdf
│   ├── memoria.tex
│   └── README.md
├── .gitignore
└── README.md
```

---

## Pipeline de datos

### 1. Landing Zone

Los datos se generan de forma sintética y se cargan en un volumen de Unity Catalog:

```text
/Volumes/workspace/telco_churn/landing_zone
```

Estructura esperada:

```text
landing_zone/
├── context/
│   └── customers.csv
├── events/
│   ├── usage/YYYY/MM/data.json
│   ├── labels/YYYY/MM/data.json
│   └── interactions/YYYY/MM/data.json
└── source_buffer/
```

### 2. Bronze Layer

La capa Bronze conserva los datos de entrada con mínima transformación y añade trazabilidad técnica.

Tablas principales:

- `bronze_customers`
- `bronze_usage`
- `bronze_labels`
- `bronze_interactions`

Características principales:

- ingesta batch para el maestro de clientes;
- ingesta incremental para eventos JSON;
- uso de Auto Loader;
- columnas de auditoría: `ingestion_timestamp`, `source_file`, `_rescued_data`.

### 3. Silver Layer

La capa Silver transforma datos crudos en entidades limpias, validadas y trazables.

Tablas principales:

- `silver_customers_history`
- `silver_churn_events`
- `silver_interactions_clean`
- `silver_quarantine_customers`
- `silver_quarantine_usage`
- `silver_quarantine_labels`
- `silver_quarantine_interactions`

Características principales:

- reglas de calidad declarativas;
- DLQ / cuarentena para registros inválidos;
- histórico SCD2 de clientes;
- AUTO CDC para mantener versiones temporales;
- normalización de timestamps;
- unión de eventos de uso y etiquetas de churn.

### 4. Gold Layer

La capa Gold prepara los datos para analítica y machine learning.

Tablas principales:

- `gold_churn_spine`
- `gold_customer_profile`
- `gold_customer_aggregations`

Funciones principales:

- construir la tabla spine cliente-mes;
- generar variable objetivo `label_will_churn`;
- preparar features de perfil de cliente;
- preparar features mensuales de uso, facturación y comportamiento;
- habilitar claves temporales para point-in-time joins.

---

## Modelado Machine Learning

El modelo predice si un cliente tendrá churn en un mes determinado.

La tabla de entrenamiento se construye desde `gold_churn_spine`, `gold_customer_profile` y `gold_customer_aggregations`.

### Dataset de entrenamiento

| Métrica | Valor |
|---|---|
| Filas | 16.316.445 |
| Clientes distintos | 2.042.162 |
| Features | 33 |
| Ventana temporal | 2023-07 a 2024-12 |
| Churn positivo | 4,83% |
| No churn | 95,17% |

El problema está desbalanceado, por lo que se aplican pesos inversos de frecuencia.

### Pipeline ML

El modelo se implementa como un pipeline completo de Spark MLlib:

1. Imputación de variables numéricas.
2. Conversión de variables binarias.
3. Ingeniería de variables derivadas.
4. Indexación de variables categóricas.
5. One-Hot Encoding.
6. Ensamblado de vector de características.
7. Selección por varianza.
8. Escalado estándar.
9. Regresión logística.

Se eligió regresión logística como baseline interpretable, robusto y escalable.

### Experimentación con MLflow

La experimentación se gestiona con MLflow:

- registro de hiperparámetros;
- métricas de entrenamiento y validación;
- comparación de modelos;
- trazabilidad de artefactos;
- registro en Unity Catalog.

Mejor configuración validada:

| Parámetro | Valor |
|---|---|
| reg_param | 0.01 |
| elastic_net_param | 0.5 |
| max_iter | 100 |
| Umbral óptimo | 0.64 |

Métricas de validación:

| Métrica | Valor |
|---|---|
| AUC-PR | 0.990393 |
| AUC-ROC | 0.999233 |
| F1 | 0.995061 |
| Precisión | 0.995162 |
| Recall | 0.995007 |

---

## Registro y gobierno del modelo

El modelo se registra en Unity Catalog como:

```text
workspace.telco_churn.churn_lr_pipeline
```

El ciclo de gobierno usa aliases:

| Alias | Significado |
|---|---|
| `candidate` | Modelo candidato recién entrenado |
| `challenger` | Modelo en comparación contra producción |
| `champion` | Modelo aprobado para inferencia |
| `rejected` | Modelo evaluado y rechazado |

Estado final validado:

| Alias | Versión | Estado |
|---|---|---|
| champion | 2 | Modelo en producción |
| rejected | 3 | Candidato descartado por no mejorar |

---

## Inferencia y despliegue operativo

La inferencia batch se ejecuta sobre nuevos datos de producción simulada.

El modelo se carga usando el alias:

```text
models:/workspace.telco_churn.churn_lr_pipeline@champion
```

Para cada cliente-mes se calculan: `prob_churn`, `prediction`, `model_version`, `inference_timestamp`.

La salida se guarda en `workspace.telco_churn.gold_churn_inference_enriched`.

Esta tabla es incremental: primero almacena predicciones y posteriormente se enriquece con la etiqueta real `label_will_churn` cuando está disponible.

### Resultados de producción simulada

Validación sobre producción simulada de 2025:

| Métrica | Valor |
|---|---|
| Filas inferidas | 1.937.801 |
| Clientes distintos | 1.066.410 |
| Ventana de producción | 2025-01 a 2025-02 |
| Versión de modelo usada | 2 |
| Filas con etiqueta real | 1.937.801 |

Distribución de predicciones:

| Predicción | Filas | Probabilidad media de churn | Churn observado |
|---|---|---|---|
| 0 | 1.683.047 | 0,0726 | 1,04% |
| 1 | 254.754 | 0,9600 | 45,04% |

El grupo marcado como riesgo concentra una tasa real de churn muy superior al grupo marcado como no riesgo, lo que permite priorizar campañas de retención.

---

## Monitorización

La tabla de inferencia enriquecida se monitoriza con Lakehouse Monitoring.

| Elemento | Valor |
|---|---|
| Tabla monitorizada | `workspace.telco_churn.gold_churn_inference_enriched` |
| Tipo de problema | Clasificación |
| Columna temporal | `inference_timestamp` |
| Predicción | `prediction` |
| Etiqueta real | `label_will_churn` |
| Modelo | `model_version` |
| Baseline | `workspace.telco_churn.gold_churn_test_baseline` |

Tablas generadas:

- `gold_churn_inference_enriched_profile_metrics`
- `gold_churn_inference_enriched_drift_metrics`

Métricas globales observadas:

| Métrica | Valor |
|---|---|
| Accuracy | 0,9187 |
| F1 ponderado | 0,9301 |
| Precisión ponderada | 0,9528 |
| Recall ponderado | 0,9187 |

También se analizan segmentos o slices para detectar degradaciones locales. Un hallazgo relevante es que el modelo rinde peor en clientes con retrasos de pago, lo que abre una línea clara de mejora.

### Alertas operativas

| Alerta | Condición |
|---|---|
| Accuracy | Accuracy inferior a 0,85 |
| Drift | JS distance superior a 0,05 |
| Completitud | Completitud inferior a 0,95 |
| Volumen | Z-score de volumen superior a 2 |

Estas alertas cubren cuatro familias de fallo:

- degradación de rendimiento del modelo;
- cambio de distribución de datos;
- roturas de calidad o nulos inesperados;
- cambios bruscos en volumen de entrada.

---

## Ejecución reproducible

Los comandos principales se ejecutan desde `codigo/`.

### Validar y desplegar bundle

```bash
databricks bundle validate -t dev -p <perfil>
databricks bundle deploy -t dev -p <perfil>
databricks bundle summary -t dev -p <perfil>
```

### Generar datos sintéticos

```bash
python -u src/medallion_pipeline/utilities/generate.py
```

### Ejecutar pipeline Medallion

```bash
databricks bundle run telco_churn -t dev -p <perfil>
```

### Ejecutar orquestación diaria

```bash
databricks bundle run telco_churn_orchestration -t dev -p <perfil>
```

### Ejecutar ciclo ML

```bash
databricks bundle run telco_churn_ml_orchestration -t dev -p <perfil>
```

### Ejecutar simulación de producción

```bash
databricks bundle run telco_churn_simulation -t dev -p <perfil>
```

---

## Validaciones SQL útiles

### Tabla de inferencia

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT customer_id) AS distinct_customers,
  MIN(year_month) AS min_month,
  MAX(year_month) AS max_month,
  SUM(CASE WHEN label_will_churn IS NOT NULL THEN 1 ELSE 0 END) AS labeled_rows,
  COUNT(DISTINCT model_version) AS model_versions
FROM workspace.telco_churn.gold_churn_inference_enriched;
```

### Distribución de predicciones

```sql
SELECT
  model_version,
  prediction,
  COUNT(*) AS rows,
  AVG(prob_churn) AS avg_prob_churn,
  AVG(CAST(label_will_churn AS DOUBLE)) AS observed_churn_rate
FROM workspace.telco_churn.gold_churn_inference_enriched
GROUP BY model_version, prediction
ORDER BY model_version, prediction;
```

### Métricas globales de monitorización

```sql
WITH latest AS (
  SELECT MAX(window.start) AS max_window
  FROM workspace.telco_churn.gold_churn_inference_enriched_profile_metrics
)
SELECT
  CAST(window.start AS STRING) AS window_start,
  model_version,
  count AS row_count,
  accuracy_score,
  f1_score.weighted AS weighted_f1,
  precision.weighted AS weighted_precision,
  recall.weighted AS weighted_recall
FROM workspace.telco_churn.gold_churn_inference_enriched_profile_metrics
WHERE window.start = (SELECT max_window FROM latest)
  AND column_name = ':table'
  AND slice_key IS NULL
ORDER BY model_version;
```

---

## Principales decisiones técnicas

### Batch frente a tiempo real estricto

El caso de churn no exige latencia de milisegundos. La decisión de inferencia batch reduce coste y complejidad, manteniendo valor de negocio suficiente para campañas periódicas de retención.

### Medallion Architecture

La arquitectura por capas permite separar responsabilidades:

- **Bronze**: dato crudo y trazabilidad.
- **Silver**: calidad, normalización e histórico.
- **Gold**: features y preparación para ML.

### Point-in-time correctness

El entrenamiento usa claves temporales para evitar que el modelo vea información futura. Esto reduce el riesgo de data leakage y hace que las métricas sean más realistas.

### Separación training-serving

El entrenamiento y la inferencia tienen ritmos diferentes:

- entrenamiento/reentrenamiento: job ML;
- inferencia: job diario de datos;
- producción: solo consume el modelo `champion`.

### Idempotencia

El diseño evita duplicados ante reintentos:

- la simulación no reinyecta eventos ya copiados;
- la inferencia no vuelve a insertar clientes ya puntuados;
- el enriquecimiento solo actualiza filas sin etiqueta real.

---

## Riesgos y mejoras futuras

Este proyecto cubre el ciclo mínimo de una solución Big Data/MLOps, pero no debe presentarse como una plataforma empresarial completa.

Mejoras propuestas:

- añadir CI/CD completo con GitHub Actions;
- crear entornos separados dev, pre y prod;
- incorporar tests automáticos de datos y pipeline;
- añadir modelos challenger más complejos, como Gradient Boosted Trees;
- mejorar explainability con SHAP o análisis de coeficientes;
- reforzar fairness por segmentos sensibles;
- ampliar features rolling históricas;
- monitorizar coste por ejecución;
- versionar contratos de datos;
- automatizar políticas de reentrenamiento controlado.

---

## Valor profesional del proyecto

Este repositorio evidencia experiencia práctica en un ciclo completo de datos y ML:

- diseño de solución Big Data orientada a negocio;
- implementación de pipelines distribuidos;
- gobierno de datos y modelos;
- despliegue reproducible;
- modelado machine learning con trazabilidad;
- operación e inferencia batch;
- monitorización de producción;
- documentación técnica incremental.

Es especialmente relevante para roles como:

- Data Engineer;
- Big Data Engineer;
- Analytics Engineer;
- MLOps Engineer junior;
- Data Platform Engineer junior;
- Cloud Data Engineer;
- Consultor de datos;
- Ingeniero de proyectos Big Data.

---

## Documentación principal

- [codigo/README.md](codigo/README.md): guía técnica de ejecución reproducible.
- [codigo/src/medallion_pipeline/README.md](codigo/src/medallion_pipeline/README.md): explicación del pipeline Medallion.
- [documentacion/memoria.md](documentacion/memoria.md): memoria técnica incremental completa (Hitos 1, 2, 3 y 4).

---

> Proyecto desarrollado en el contexto de la asignatura **Desarrollo y Despliegue de Soluciones Big Data** del Máster Universitario en Big Data y Computación en la Nube.
>
> Aunque parte de los datos son sintéticos, el diseño reproduce problemas reales de ingeniería de datos y MLOps: volumen, drift, calidad, trazabilidad, idempotencia, separación training-serving, gobierno de modelo y monitorización.

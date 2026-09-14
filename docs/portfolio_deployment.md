# Publicación documental

GitHub Pages expone la arquitectura, las métricas de producción simulada, las capturas de MLflow y Unity Catalog y la memoria del proyecto sin depender de un workspace Databricks activo.

El workflow `.github/workflows/pages.yml` construye el repositorio con Jekyll y publica `https://alonsomarcosm.github.io/databricks-telco-churn-lakehouse/`.

El caso de estudio canónico y bilingüe vive en `https://alonsomarcosm.github.io/es/projects/telco-churn-mlops-databricks/`. El portfolio conserva una ruta estática para el nombre antiguo.

La web no ejecuta pipelines ni modelos. La reproducción real continúa mediante Databricks Asset Bundles y los comandos documentados en `codigo/`.

La última verificación pública deberá renovarse después del despliegue del repositorio renombrado.

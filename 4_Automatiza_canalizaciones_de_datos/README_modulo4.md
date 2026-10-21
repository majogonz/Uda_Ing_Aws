# Automatiza canalizaciones de datos (Apache Airflow)

## Resumen del proyecto

STEDI/Sparkify necesita automatizar su pipeline ETL con Apache Airflow. Este proyecto construye un DAG que carga datos crudos desde S3 (`s3://udacity-dend/log_data` y `s3://udacity-dend/song_data`) a tablas de staging en Amazon Redshift, los transforma en un esquema en estrella (tabla de hechos `songplays` y tablas de dimensiones `users`, `songs`, `artists`, `time`), y finaliza con controles de calidad de datos.

## Arquitectura del DAG

```
Begin_execution
   ├── Stage_events ──┐
   └── Stage_songs  ──┴── Load_songplays_fact_table
                              ├── Load_user_dim_table
                              ├── Load_song_dim_table
                              ├── Load_artist_dim_table
                              └── Load_time_dim_table
                                        └── Run_data_quality_checks
                                                 └── Stop_execution
```

## Operadores personalizados

- **StageToRedshiftOperator**: copia datos JSON desde S3 a una tabla de staging en Redshift (limpia la tabla antes de cargar).
- **LoadFactOperator**: inserta datos en la tabla de hechos `songplays` a partir de un SQL de transformación.
- **LoadDimensionOperator**: inserta datos en las tablas de dimensiones, con opción de truncar antes de cargar (patrón truncate-insert).
- **DataQualityOperator**: valida que cada tabla final tenga al menos una fila; falla el DAG si alguna verificación no pasa.

## Infraestructura usada

- Amazon Redshift Serverless (`default-workgroup`, 8 RPU de capacidad base, accesible públicamente para la conexión desde el espacio de trabajo de Airflow).
- Usuario IAM `airflow-redshift-user` con permisos `AmazonS3ReadOnlyAccess` y `AmazonRedshiftFullAccess`, usado en la conexión `aws_credentials` de Airflow.
- Conexión `redshift` apuntando al endpoint del workgroup.

## Verificación de resultados

Conteos finales de filas tras una ejecución completa y limpia del DAG:

| Tabla | Filas |
|---|---|
| staging_events | 8,056 |
| staging_songs | 14,896 |
| songplays | 20,460 |
| users | 104 |
| songs | 14,896 |
| artists | 10,025 |
| time | 20,460 |

Todas las tareas del DAG (`Begin_execution`, `Stage_events`, `Stage_songs`, `Load_songplays_fact_table`, `Load_user_dim_table`, `Load_song_dim_table`, `Load_artist_dim_table`, `Load_time_dim_table`, `Run_data_quality_checks`, `Stop_execution`) terminaron en estado `success`, confirmado vía `airflow tasks states-for-dag-run` y verificación directa de los conteos con la API de datos de Redshift (`aws redshift-data`).

## Archivos del repositorio

- `final_project.py`: DAG principal con las dependencias de tareas.
- `stage_redshift.py`, `load_fact.py`, `load_dimension.py`, `data_quality.py`: los 4 operadores personalizados (van en `airflow/plugins/final_project_operators/` del espacio de trabajo).

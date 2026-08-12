# Data Warehouse con Amazon Redshift - Sparkify

## Resumen del Proyecto

Sparkify es una startup de streaming de música que quiere analizar el comportamiento de sus usuarios. Sus datos residen en Amazon S3 como archivos JSON: registros de actividad de los usuarios en la app (`log_data`) y metadatos de las canciones de su catálogo (`song_data`).

Este proyecto construye un data warehouse en Amazon Redshift: carga esos datos JSON crudos desde S3 a tablas de staging usando el comando `COPY`, y luego los transforma mediante SQL en un esquema en estrella optimizado para que el equipo de analítica pueda consultar fácilmente qué canciones escuchan sus usuarios.

## Esquema de la Base de Datos

**Tablas de staging** (datos crudos, tal como llegan de S3):
- `staging_events`: eventos de actividad de la app (reproducciones, logins, etc.)
- `staging_songs`: metadatos de canciones y artistas

**Tabla de hechos:**
- `songplays`: cada reproducción de una canción (`page = 'NextSong'`), con claves foráneas hacia todas las dimensiones.

**Tablas de dimensiones:**
- `users`: usuarios de la app
- `songs`: catálogo de canciones
- `artists`: catálogo de artistas
- `time`: desglose de cada timestamp en hora/día/semana/mes/año/día de la semana

El JOIN entre `staging_events` y `staging_songs` para poblar `songplays` usa tres condiciones (título de canción, nombre de artista y duración) porque los datos de staging no comparten un ID común; esa combinación es la única forma de emparejar de forma confiable un evento de reproducción con la canción correspondiente.

## Cómo Ejecutar

1. Copia `dwh.cfg.example` a `dwh.cfg` y completa tus propias credenciales de Redshift y el ARN de tu rol IAM.
2. Crea las tablas: `python create_tables.py`
3. Ejecuta el ETL: `python etl.py`

## Archivos del Repositorio

- `create_tables.py`: elimina las tablas si existen y las vuelve a crear (staging + esquema en estrella).
- `etl.py`: carga las tablas de staging desde S3 con `COPY`, y luego inserta/transforma los datos hacia las tablas finales.
- `sql_queries.py`: contiene todas las sentencias SQL del proyecto (DROP, CREATE, COPY, INSERT) usadas por los dos scripts anteriores.
- `dwh.cfg.example`: plantilla de configuración. El archivo `dwh.cfg` real (con credenciales) no se versiona por seguridad.

## Verificación

Se verificó manualmente que el pipeline completo corre sin errores, con los siguientes conteos de filas resultantes:

| Tabla | Filas |
|---|---|
| staging_events | 8,056 |
| staging_songs | 14,896 |
| songplays | 319 |
| users | 104 |
| songs | 14,896 |
| artists | 10,025 |
| time | 319 |

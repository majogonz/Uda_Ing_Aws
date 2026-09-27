# Analisis del Equilibrio Humano de STEDI

## Resumen del Proyecto

STEDI es una startup que vende un entrenador de equilibrio (Step Trainer) junto con una app movil que usa el acelerometro del telefono para detectar movimientos de alto riesgo durante los ejercicios. Este proyecto construye un lakehouse en AWS (S3 + Glue + Athena) que integra tres fuentes de datos (clientes, acelerometro y Step Trainer) en tres zonas: aterrizaje (landing), confiable (trusted) y curada (curated), respetando en todo momento el consentimiento de cada cliente para compartir sus datos con fines de investigacion.

## Arquitectura de Datos

**Zona de aterrizaje** (datos crudos desde S3, tal como llegan):
- `customer_landing`: registros de clientes del sitio web
- `accelerometer_landing`: lecturas del acelerometro de la app movil
- `step_trainer_landing`: lecturas del sensor IoT del Step Trainer

**Zona confiable** (solo datos de clientes que aceptaron compartir informacion para investigacion):
- `customer_trusted`: clientes con `shareWithResearchAsOfDate` no nulo
- `accelerometer_trusted`: lecturas de acelerometro de esos clientes
- `step_trainer_trusted`: lecturas del Step Trainer de clientes en `customers_curated`

**Zona curada** (datos listos para analisis/machine learning):
- `customers_curated`: clientes que tienen datos de acelerometro Y aceptaron compartir datos
- `machine_learning_curated`: lecturas del Step Trainer unidas con las de acelerometro por marca de tiempo

## Como Ejecutar

1. Crear un bucket de S3 y cargar los tres datasets de partida en `customer_landing/`, `accelerometer_landing/` y `step_trainer_landing/`.
2. Ejecutar los scripts SQL (`customer_landing.sql`, `accelerometer_landing.sql`, `step_trainer_landing.sql`) en el editor de consultas de Athena para crear las tablas Glue de la zona de aterrizaje.
3. Ejecutar en orden los 5 jobs de Glue (Python/Spark):
   - `customer_landing_to_trusted.py`
   - `accelerometer_landing_to_trusted.py`
   - `customer_trusted_to_curated.py`
   - `step_trainer_landing_to_trusted.py`
   - `machine_learning_curated.py`

## Archivos del Repositorio

- `customer_landing.sql`, `accelerometer_landing.sql`, `step_trainer_landing.sql`: DDL para crear las tablas Glue de la zona de aterrizaje.
- `customer_landing_to_trusted.py`: filtra clientes que aceptaron compartir datos para investigacion.
- `accelerometer_landing_to_trusted.py`: filtra lecturas de acelerometro de esos clientes.
- `customer_trusted_to_curated.py`: cruza clientes confiables con los que tienen datos de acelerometro.
- `step_trainer_landing_to_trusted.py`: filtra lecturas del Step Trainer de clientes curados.
- `machine_learning_curated.py`: une lecturas de Step Trainer y acelerometro por marca de tiempo.

## Verificacion

Conteos de filas verificados en cada etapa (coinciden exactamente con los esperados):

| Zona | Tabla | Filas |
|---|---|---|
| Aterrizaje | customer_landing | 956 |
| Aterrizaje | accelerometer_landing | 81,273 |
| Aterrizaje | step_trainer_landing | 28,680 |
| Confiable | customer_trusted | 482 |
| Confiable | accelerometer_trusted | 40,981 |
| Confiable | step_trainer_trusted | 14,460 |
| Curada | customers_curated | 482 |
| Curada | machine_learning_curated | 43,681 |

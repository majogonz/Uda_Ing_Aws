CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    `user` string,
    `timestamp` bigint,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-lakehouse-majogonz2026/accelerometer_landing/landing/';

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer acelerometro (zona de aterrizaje) y clientes confiables (zona confiable)
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing"
).toDF()

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
).toDF()

# Solo lecturas de acelerometro de clientes que aceptaron compartir sus datos
accelerometer_trusted_df = accelerometer_landing.join(
    customer_trusted,
    accelerometer_landing["user"] == customer_trusted["email"],
    "inner"
).select(
    accelerometer_landing["user"],
    accelerometer_landing["timestamp"],
    accelerometer_landing["x"],
    accelerometer_landing["y"],
    accelerometer_landing["z"]
)

from awsglue.dynamicframe import DynamicFrame
accelerometer_trusted = DynamicFrame.fromDF(accelerometer_trusted_df, glueContext, "accelerometer_trusted")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-majogonzv2/accelerometer_trusted/",
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
sink.setFormat("json")
sink.writeFrame(accelerometer_trusted)

job.commit()

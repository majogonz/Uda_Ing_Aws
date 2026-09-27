import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
).toDF()

customers_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated"
).toDF()

# Solo datos del Step Trainer de clientes que estan en customers_curated
step_trainer_trusted_df = step_trainer_landing.join(
    customers_curated,
    step_trainer_landing["serialNumber"] == customers_curated["serialNumber"],
    "inner"
).select(
    step_trainer_landing["sensorReadingTime"],
    step_trainer_landing["serialNumber"],
    step_trainer_landing["distanceFromObject"]
)

step_trainer_trusted = DynamicFrame.fromDF(step_trainer_trusted_df, glueContext, "step_trainer_trusted")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-majogonzv2/step_trainer_trusted/",
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
sink.setFormat("json")
sink.writeFrame(step_trainer_trusted)

job.commit()

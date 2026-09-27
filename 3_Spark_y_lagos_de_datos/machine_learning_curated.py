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

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
).toDF()

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
).toDF()

# Une cada lectura del Step Trainer con la lectura de acelerometro de la misma marca de tiempo
machine_learning_curated_df = step_trainer_trusted.join(
    accelerometer_trusted,
    step_trainer_trusted["sensorReadingTime"] == accelerometer_trusted["timestamp"],
    "inner"
)

machine_learning_curated = DynamicFrame.fromDF(machine_learning_curated_df, glueContext, "machine_learning_curated")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-majogonzv2/machine_learning_curated/",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
sink.setFormat("json")
sink.writeFrame(machine_learning_curated)

job.commit()

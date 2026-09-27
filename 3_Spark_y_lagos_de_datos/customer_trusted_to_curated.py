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

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
).toDF()

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
).toDF()

# Solo clientes que tienen datos de acelerometro Y aceptaron compartir para investigacion
accel_users = accelerometer_trusted.select("user").distinct()

customers_curated_df = customer_trusted.join(
    accel_users,
    customer_trusted["email"] == accel_users["user"],
    "inner"
).select(customer_trusted["*"])

customers_curated = DynamicFrame.fromDF(customers_curated_df, glueContext, "customers_curated")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-majogonzv2/customers_curated/",
    enableUpdateCatalog=True,
    transformation_ctx="customers_curated_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customers_curated")
sink.setFormat("json")
sink.writeFrame(customers_curated)

job.commit()

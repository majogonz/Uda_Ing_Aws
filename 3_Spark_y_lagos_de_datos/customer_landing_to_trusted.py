import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer la zona de aterrizaje de clientes desde el catalogo de Glue
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing"
)

# Sanitizar: solo clientes que aceptaron compartir datos para investigacion
customer_trusted = customer_landing.filter(
    lambda row: row["shareWithResearchAsOfDate"] is not None
)

# Escribir en S3 y registrar/actualizar la tabla Glue "customer_trusted"
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-majogonzv2/customer_trusted/",
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
sink.setFormat("json")
sink.writeFrame(customer_trusted)

job.commit()

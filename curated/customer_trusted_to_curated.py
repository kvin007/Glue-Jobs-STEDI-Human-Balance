import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1686601650915 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi_project",
        table_name="customer_trusted",
        transformation_ctx="customer_trusted_node1686601650915",
    )
)

# Script generated for node S3_accelerometer_landing
S3_accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kvin007-glue-bucket/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3_accelerometer_landing_node1",
)

# Script generated for node Join on email
Joinonemail_node1686601778495 = Join.apply(
    frame1=S3_accelerometer_landing_node1,
    frame2=customer_trusted_node1686601650915,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Joinonemail_node1686601778495",
)

# Script generated for node Drop Fields
DropFields_node1686601871640 = DropFields.apply(
    frame=Joinonemail_node1686601778495,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1686601871640",
)

# Script generated for node S3_customers_curated
S3_customers_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686601871640,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kvin007-glue-bucket/customers_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3_customers_curated_node3",
)

job.commit()

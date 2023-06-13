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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1686599710763 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi_project",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLanding_node1686599710763",
    )
)

# Script generated for node S3_customer_trusted
S3_customer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kvin007-glue-bucket/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3_customer_trusted_node1",
)

# Script generated for node Join
Join_node1686599731265 = Join.apply(
    frame1=S3_customer_trusted_node1,
    frame2=AccelerometerLanding_node1686599710763,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686599731265",
)

# Script generated for node Drop Fields
DropFields_node1686599898892 = DropFields.apply(
    frame=Join_node1686599731265,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1686599898892",
)

# Script generated for node S3_accelerometer_trusted
S3_accelerometer_trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686599898892,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://kvin007-glue-bucket/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3_accelerometer_trusted_node3",
)

job.commit()

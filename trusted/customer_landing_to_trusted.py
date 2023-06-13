import re
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

# Script generated for node S3_customer_landing
S3_customer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://kvin007-glue-bucket/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3_customer_landing_node1",
)

# Script generated for node Filter
Filter_node1686598727861 = Filter.apply(
    frame=S3_customer_landing_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1686598727861",
)

# Script generated for node S3_customer_trusted
S3_customer_trusted_node1686598732814 = (
    glueContext.write_dynamic_frame.from_options(
        frame=Filter_node1686598727861,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://kvin007-glue-bucket/customer_trusted/",
            "partitionKeys": [],
        },
        transformation_ctx="S3_customer_trusted_node1686598732814",
    )
)

job.commit()

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

# Script generated for node step_trainer_landing
step_trainer_landing_node1686616525617 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://kvin007-glue-bucket/step_trainer_landing/"],
            "recurse": True,
        },
        transformation_ctx="step_trainer_landing_node1686616525617",
    )
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1686616420969 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi_project",
        table_name="accelerometer_trusted",
        transformation_ctx="accelerometer_trusted_node1686616420969",
    )
)

# Script generated for node Join on sensorReadingTime
JoinonsensorReadingTime_node1686616553340 = Join.apply(
    frame1=accelerometer_trusted_node1686616420969,
    frame2=step_trainer_landing_node1686616525617,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinonsensorReadingTime_node1686616553340",
)

# Script generated for node S3_machine_learning_curated
S3_machine_learning_curated_node1686679438017 = (
    glueContext.write_dynamic_frame.from_options(
        frame=JoinonsensorReadingTime_node1686616553340,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://kvin007-glue-bucket/machine_learning_curated/",
            "partitionKeys": [],
        },
        transformation_ctx="S3_machine_learning_curated_node1686679438017",
    )
)

job.commit()

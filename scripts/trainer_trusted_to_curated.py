import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted Source
StepTrainerTrustedSource_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="anhtu12st",
    table_name="step_trainer_trusted_zone",
    transformation_ctx="StepTrainerTrustedSource_node1",
)

# Script generated for node Accelerometer Trusted Source
AccelerometerTrustedSource_node1678630297028 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="anhtu12st",
        table_name="accelerometer_trusted_zone",
        transformation_ctx="AccelerometerTrustedSource_node1678630297028",
    )
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=AccelerometerTrustedSource_node1678630297028,
    frame2=StepTrainerTrustedSource_node1,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1678630760780 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=["user"],
    transformation_ctx="DropFields_node1678630760780",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacity-anhtu12st/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="anhtu12st", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1678630760780)
job.commit()

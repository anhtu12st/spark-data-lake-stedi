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

# Script generated for node Step Trainer Landing Source
StepTrainerLandingSource_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-anhtu12st/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingSource_node1",
)

# Script generated for node Customer Curated Source
CustomerCuratedSource_node1678630297028 = glueContext.create_dynamic_frame.from_catalog(
    database="anhtu12st",
    table_name="customer_curated_zone",
    transformation_ctx="CustomerCuratedSource_node1678630297028",
)

# Script generated for node Join
Join_node1678632182873 = Join.apply(
    frame1=CustomerCuratedSource_node1678630297028,
    frame2=StepTrainerLandingSource_node1,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1678632182873",
)

# Script generated for node Drop Fields
DropFields_node1678632530493 = DropFields.apply(
    frame=Join_node1678632182873,
    paths=[
        "serialnumber",
        "timestamp",
        "birthday",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1678632530493",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1678632554221 = glueContext.getSink(
    path="s3://udacity-anhtu12st/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedZone_node1678632554221",
)
StepTrainerTrustedZone_node1678632554221.setCatalogInfo(
    catalogDatabase="anhtu12st", catalogTableName="step_trainer_trusted_zone"
)
StepTrainerTrustedZone_node1678632554221.setFormat("json")
StepTrainerTrustedZone_node1678632554221.writeFrame(DropFields_node1678632530493)
job.commit()

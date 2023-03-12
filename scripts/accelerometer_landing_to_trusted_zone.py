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

# Script generated for node Customer Trusted Zone Source
CustomerTrustedZoneSource_node1678552126921 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="anhtu12st",
        table_name="customer_trusted_zone",
        transformation_ctx="CustomerTrustedZoneSource_node1678552126921",
    )
)

# Script generated for node Accelerometer Landing Zone Source
AccelerometerLandingZoneSource_node1678549905881 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="anhtu12st",
        table_name="accelerometer_landing_zone",
        transformation_ctx="AccelerometerLandingZoneSource_node1678549905881",
    )
)

# Script generated for node Join Customer Node
JoinCustomerNode_node1678549795163 = Join.apply(
    frame1=AccelerometerLandingZoneSource_node1678549905881,
    frame2=CustomerTrustedZoneSource_node1678552126921,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomerNode_node1678549795163",
)

# Script generated for node Drop Fields
DropFields_node1678552880580 = DropFields.apply(
    frame=JoinCustomerNode_node1678549795163,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1678552880580",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1678550240450 = glueContext.getSink(
    path="s3://udacity-anhtu12st/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrustedZone_node1678550240450",
)
AccelerometerTrustedZone_node1678550240450.setCatalogInfo(
    catalogDatabase="anhtu12st", catalogTableName="accelerometer_trusted_zone"
)
AccelerometerTrustedZone_node1678550240450.setFormat("json")
AccelerometerTrustedZone_node1678550240450.writeFrame(DropFields_node1678552880580)
job.commit()

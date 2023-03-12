import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone Source
CustomerLandingZoneSource_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-anhtu12st/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZoneSource_node1",
)

# Script generated for node Filter Privacy User
FilterPrivacyUser_node1678603533525 = Filter.apply(
    frame=CustomerLandingZoneSource_node1,
    f=lambda row: (row["shareWithResearchAsOfDate"] > 0),
    transformation_ctx="FilterPrivacyUser_node1678603533525",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1678603587637 = glueContext.getSink(
    path="s3://udacity-anhtu12st/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedZone_node1678603587637",
)
CustomerTrustedZone_node1678603587637.setCatalogInfo(
    catalogDatabase="anhtu12st", catalogTableName="customer_trusted_zone"
)
CustomerTrustedZone_node1678603587637.setFormat("json")
CustomerTrustedZone_node1678603587637.writeFrame(FilterPrivacyUser_node1678603533525)
job.commit()

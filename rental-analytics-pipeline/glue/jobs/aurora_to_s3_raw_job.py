import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node RelationalDB.bookings
RelationalDBbookings_node1750250815093 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBbookings_node1750250815093"
)

# Script generated for node RelationalDB.apartments
RelationalDBapartments_node1750251043020 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBapartments_node1750251043020"
)

# Script generated for node RelationalDB.apartment_attributes
RelationalDBapartment_attributes_node1750251160116 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBapartment_attributes_node1750251160116"
)

# Script generated for node RelationalDB.userViewing
RelationalDBuserViewing_node1750251311720 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewing",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDBuserViewing_node1750251311720"
)

# Script generated for node S3-Bookings
EvaluateDataQuality().process_rows(frame=RelationalDBbookings_node1750250815093, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBbookings_node1750250815093.count() >= 1):
   RelationalDBbookings_node1750250815093 = RelationalDBbookings_node1750250815093.coalesce(1)
S3Bookings_node1750160729728 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBbookings_node1750250815093, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Bookings_node1750160729728")

# Script generated for node S3-Apartments
EvaluateDataQuality().process_rows(frame=RelationalDBapartments_node1750251043020, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBapartments_node1750251043020.count() >= 1):
   RelationalDBapartments_node1750251043020 = RelationalDBapartments_node1750251043020.coalesce(1)
S3Apartments_node1750160733709 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBapartments_node1750251043020, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Apartments_node1750160733709")

# Script generated for node S3-ApartmentsAttributes
EvaluateDataQuality().process_rows(frame=RelationalDBapartment_attributes_node1750251160116, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBapartment_attributes_node1750251160116.count() >= 1):
   RelationalDBapartment_attributes_node1750251160116 = RelationalDBapartment_attributes_node1750251160116.coalesce(1)
S3ApartmentsAttributes_node1750160723940 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBapartment_attributes_node1750251160116, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3ApartmentsAttributes_node1750160723940")

# Script generated for node S3-UserViewings
EvaluateDataQuality().process_rows(frame=RelationalDBuserViewing_node1750251311720, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDBuserViewing_node1750251311720.count() >= 1):
   RelationalDBuserViewing_node1750251311720 = RelationalDBuserViewing_node1750251311720.coalesce(1)
S3UserViewings_node1750160731898 = glueContext.write_dynamic_frame.from_options(frame=RelationalDBuserViewing_node1750251311720, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/user_viewings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3UserViewings_node1750160731898")

job.commit()
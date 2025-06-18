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

# Script generated for node UserViewings-Rentaldb
UserViewingsRentaldb_node1750160104828 = glueContext.create_dynamic_frame.from_catalog(database="rentaldb_raw", table_name="rentaldb_user_viewing", transformation_ctx="UserViewingsRentaldb_node1750160104828")

# Script generated for node Appartments-Rentaldb
AppartmentsRentaldb_node1750160088103 = glueContext.create_dynamic_frame.from_catalog(database="rentaldb_raw", table_name="rentaldb_apartments", transformation_ctx="AppartmentsRentaldb_node1750160088103")

# Script generated for node AppartmentAttributes-Rentaldb
AppartmentAttributesRentaldb_node1750160101689 = glueContext.create_dynamic_frame.from_catalog(database="rentaldb_raw", table_name="rentaldb_apartment_attributes", transformation_ctx="AppartmentAttributesRentaldb_node1750160101689")

# Script generated for node Bookings-Rentaldb
BookingsRentaldb_node1750160103419 = glueContext.create_dynamic_frame.from_catalog(database="rentaldb_raw", table_name="rentaldb_bookings", transformation_ctx="BookingsRentaldb_node1750160103419")

# Script generated for node S3-UserViewings
EvaluateDataQuality().process_rows(frame=UserViewingsRentaldb_node1750160104828, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (UserViewingsRentaldb_node1750160104828.count() >= 1):
   UserViewingsRentaldb_node1750160104828 = UserViewingsRentaldb_node1750160104828.coalesce(1)
S3UserViewings_node1750160731898 = glueContext.write_dynamic_frame.from_options(frame=UserViewingsRentaldb_node1750160104828, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/user_viewings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3UserViewings_node1750160731898")

# Script generated for node S3-Apartments
EvaluateDataQuality().process_rows(frame=AppartmentsRentaldb_node1750160088103, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (AppartmentsRentaldb_node1750160088103.count() >= 1):
   AppartmentsRentaldb_node1750160088103 = AppartmentsRentaldb_node1750160088103.coalesce(1)
S3Apartments_node1750160733709 = glueContext.write_dynamic_frame.from_options(frame=AppartmentsRentaldb_node1750160088103, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Apartments_node1750160733709")

# Script generated for node S3-ApartmentsAttributes
EvaluateDataQuality().process_rows(frame=AppartmentAttributesRentaldb_node1750160101689, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (AppartmentAttributesRentaldb_node1750160101689.count() >= 1):
   AppartmentAttributesRentaldb_node1750160101689 = AppartmentAttributesRentaldb_node1750160101689.coalesce(1)
S3ApartmentsAttributes_node1750160723940 = glueContext.write_dynamic_frame.from_options(frame=AppartmentAttributesRentaldb_node1750160101689, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3ApartmentsAttributes_node1750160723940")

# Script generated for node S3-Bookings
EvaluateDataQuality().process_rows(frame=BookingsRentaldb_node1750160103419, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750160050103", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (BookingsRentaldb_node1750160103419.count() >= 1):
   BookingsRentaldb_node1750160103419 = BookingsRentaldb_node1750160103419.coalesce(1)
S3Bookings_node1750160729728 = glueContext.write_dynamic_frame.from_options(frame=BookingsRentaldb_node1750160103419, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-marketplace-bucks/data/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="S3Bookings_node1750160729728")

job.commit()
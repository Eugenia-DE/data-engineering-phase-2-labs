import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node BOOKINGS
BOOKINGS_node1750182076037 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-rental-marketplace-bucks/data/raw/bookings/"], "recurse": True}, transformation_ctx="BOOKINGS_node1750182076037")

# Script generated for node USER_VIEWINGS
USER_VIEWINGS_node1750182078027 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-rental-marketplace-bucks/data/raw/user_viewings/"], "recurse": True}, transformation_ctx="USER_VIEWINGS_node1750182078027")

# Script generated for node APARTMENT_ATTRIBUTES
APARTMENT_ATTRIBUTES_node1750182070081 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-rental-marketplace-bucks/data/raw/apartment_attributes/"], "recurse": True}, transformation_ctx="APARTMENT_ATTRIBUTES_node1750182070081")

# Script generated for node S3-APARTMENTS
S3APARTMENTS_node1750182077062 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://lab2-rental-marketplace-bucks/data/raw/apartments/"], "recurse": True}, transformation_ctx="S3APARTMENTS_node1750182077062")

# Script generated for node BOOKINGS_S3_TO_RAW
BOOKINGS_S3_TO_RAW_node1750182428325 = glueContext.write_dynamic_frame.from_options(frame=BOOKINGS_node1750182076037, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.bookings (booking_id VARCHAR, user_id VARCHAR, apartment_id VARCHAR, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="BOOKINGS_S3_TO_RAW_node1750182428325")

# Script generated for node USER_VIEWINGS_S3_TO_RAW
USER_VIEWINGS_S3_TO_RAW_node1750182423639 = glueContext.write_dynamic_frame.from_options(frame=USER_VIEWINGS_node1750182078027, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.user_viewing (user_id VARCHAR, apartment_id VARCHAR, viewed_at VARCHAR, is_wishlisted VARCHAR, call_to_action VARCHAR);"}, transformation_ctx="USER_VIEWINGS_S3_TO_RAW_node1750182423639")

# Script generated for node APARTMENTS_ATTRIBUTES_S3_TO_RAW
APARTMENTS_ATTRIBUTES_S3_TO_RAW_node1750182425588 = glueContext.write_dynamic_frame.from_options(frame=APARTMENT_ATTRIBUTES_node1750182070081, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.apartment_attributes (id VARCHAR, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo VARCHAR, pets_allowed VARCHAR, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="APARTMENTS_ATTRIBUTES_S3_TO_RAW_node1750182425588")

# Script generated for node APARTMENTS-S3_TO_RAW
APARTMENTSS3_TO_RAW_node1750182426982 = glueContext.write_dynamic_frame.from_options(frame=S3APARTMENTS_node1750182077062, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.apartments (id VARCHAR, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on VARCHAR, is_active BOOLEAN, last_modified_timestamp VARCHAR);"}, transformation_ctx="APARTMENTSS3_TO_RAW_node1750182426982")

job.commit()
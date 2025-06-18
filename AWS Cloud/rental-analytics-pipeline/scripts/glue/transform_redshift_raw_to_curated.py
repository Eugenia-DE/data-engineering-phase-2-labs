import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, to_date, lower, trim

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()
    columns = df.columns

    # Apply transformations only if column exists
    if "booking_date" in columns:
        df = df.withColumn("booking_date", to_date(col("booking_date"), "dd/MM/yyyy"))
    if "checkin_date" in columns:
        df = df.withColumn("checkin_date", to_date(col("checkin_date"), "dd/MM/yyyy"))
    if "checkout_date" in columns:
        df = df.withColumn("checkout_date", to_date(col("checkout_date"), "dd/MM/yyyy"))
    if "viewed_at" in columns:
        df = df.withColumn("viewed_at", to_date(col("viewed_at"), "dd/MM/yyyy"))
    if "is_wishlisted" in columns:
        df = df.withColumn("is_wishlisted", when(col("is_wishlisted") == "TRUE", True).otherwise(False))
    if "has_photo" in columns:
        df = df.withColumn("has_photo", when(col("has_photo") == "TRUE", True).otherwise(False))
    if "pets_allowed" in columns:
        df = df.withColumn("pets_allowed", when(col("pets_allowed") == "TRUE", True).otherwise(False))
    if "category" in columns:
        df = df.withColumn("category", lower(trim(col("category"))))
    if "amenities" in columns:
        df = df.withColumn("amenities", lower(trim(col("amenities"))))
    if "price_type" in columns:
        df = df.withColumn("price_type", lower(trim(col("price_type"))))
    if "cityname" in columns:
        df = df.withColumn("cityname", lower(trim(col("cityname"))))
    if "state" in columns:
        df = df.withColumn("state", lower(trim(col("state"))))

    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "dyf_transformed")
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node apartments - redshift raw layer
apartmentsredshiftrawlayer_node1750185588756 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift_conn"}, transformation_ctx="apartmentsredshiftrawlayer_node1750185588756")

# Script generated for node apartments attributes redshift raw layer
apartmentsattributesredshiftrawlayer_node1750186006488 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift_conn"}, transformation_ctx="apartmentsattributesredshiftrawlayer_node1750186006488")

# Script generated for node bookings redshift raw layer
bookingsredshiftrawlayer_node1750185914157 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift_conn"}, transformation_ctx="bookingsredshiftrawlayer_node1750185914157")

# Script generated for node user viewings redshift raw layer
userviewingsredshiftrawlayer_node1750185968718 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift_conn"}, transformation_ctx="userviewingsredshiftrawlayer_node1750185968718")

# Script generated for node Custom Transform
CustomTransform_node1750186321963 = MyTransform(glueContext, DynamicFrameCollection({"apartmentsredshiftrawlayer_node1750185588756": apartmentsredshiftrawlayer_node1750185588756}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750186012635 = MyTransform(glueContext, DynamicFrameCollection({"apartmentsattributesredshiftrawlayer_node1750186006488": apartmentsattributesredshiftrawlayer_node1750186006488}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750185923408 = MyTransform(glueContext, DynamicFrameCollection({"bookingsredshiftrawlayer_node1750185914157": bookingsredshiftrawlayer_node1750185914157}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750185976911 = MyTransform(glueContext, DynamicFrameCollection({"userviewingsredshiftrawlayer_node1750185968718": userviewingsredshiftrawlayer_node1750185968718}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1750186340674 = SelectFromCollection.apply(dfc=CustomTransform_node1750186321963, key=list(CustomTransform_node1750186321963.keys())[0], transformation_ctx="SelectFromCollection_node1750186340674")

# Script generated for node Select From Collection
SelectFromCollection_node1750186030765 = SelectFromCollection.apply(dfc=CustomTransform_node1750186012635, key=list(CustomTransform_node1750186012635.keys())[0], transformation_ctx="SelectFromCollection_node1750186030765")

# Script generated for node Select From Collection
SelectFromCollection_node1750185946035 = SelectFromCollection.apply(dfc=CustomTransform_node1750185923408, key=list(CustomTransform_node1750185923408.keys())[0], transformation_ctx="SelectFromCollection_node1750185946035")

# Script generated for node Select From Collection
SelectFromCollection_node1750185984048 = SelectFromCollection.apply(dfc=CustomTransform_node1750185976911, key=list(CustomTransform_node1750185976911.keys())[0], transformation_ctx="SelectFromCollection_node1750185984048")

# Script generated for node apartments - curated layer
apartmentscuratedlayer_node1750186347079 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750186340674, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id BIGINT, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp DATE);"}, transformation_ctx="apartmentscuratedlayer_node1750186347079")

# Script generated for node apartment attributes curated layer
apartmentattributescuratedlayer_node1750186038014 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750186030765, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id BIGINT, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="apartmentattributescuratedlayer_node1750186038014")

# Script generated for node bookings curated layer
bookingscuratedlayer_node1750185958116 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750185946035, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id BIGINT, user_id BIGINT, apartment_id BIGINT, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="bookingscuratedlayer_node1750185958116")

# Script generated for node user viewing curated layer
userviewingcuratedlayer_node1750185992295 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750185984048, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewing", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing (user_id BIGINT, apartment_id BIGINT, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR, is_wishlisted_string VARCHAR);"}, transformation_ctx="userviewingcuratedlayer_node1750185992295")

job.commit()
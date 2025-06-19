import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Custom Transform
def TransformApartments(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date

    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    # Convert and clean data
    df_transformed = df \
        .withColumn("is_active", when(col("is_active") == "TRUE", True).otherwise(False)) \
        .withColumn("listing_created_on", to_date(col("listing_created_on"), "dd/MM/yyyy")) \
        .withColumn("last_modified_timestamp", to_date(col("last_modified_timestamp"), "dd/MM/yyyy"))

    # === Row-level filter for critical fields ===
    df_transformed = df_transformed.filter(
        (col("id").isNotNull()) &
        (~col("id").isin("@NULL@", "null", "NULL", "")) &
        (col("listing_created_on").isNotNull())  # Optional: ensure date is parsed
    )

    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
      
# Script generated for node Custom Transform
def TransformUserViewing(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date

    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    df_transformed = df \
        .withColumn("is_wishlisted", when(col("is_wishlisted") == "TRUE", True).otherwise(False)) \
        .withColumn("viewed_at", to_date(col("viewed_at"), "dd/MM/yyyy"))

    # Filter invalid rows
    df_transformed = df_transformed.filter(
        (col("user_id").isNotNull()) & (~col("user_id").isin("@NULL@", "", "null", "NULL"))) \
        .filter((col("apartment_id").isNotNull()) & (~col("apartment_id").isin("@NULL@", "", "null", "NULL"))) \
        .filter(col("viewed_at").isNotNull())

    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
# Script generated for node Custom Transform
def TransformApartmentAttributes(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date
    # Get the first (and usually only) DynamicFrame from the input collection
    dyf = dfc.select(list(dfc.keys())[0])
    # Convert to DataFrame
    df = dyf.toDF()
    # Apply transformations
    df_transformed = df \
        .withColumn("has_photo", when(col("has_photo") == "TRUE", True).otherwise(False)) \
        .withColumn("pets_allowed", when(col("pets_allowed") == "TRUE", True).otherwise(False))
    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")
    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)   
# Script generated for node Custom Transform
def TransformBookings(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, to_date
    # Get the first (and usually only) DynamicFrame from the input collection
    dyf = dfc.select(list(dfc.keys())[0])
    # Convert to DataFrame
    df = dyf.toDF()
    # Apply transformations
    df_transformed = df \
        .withColumn("booking_date", to_date(col("booking_date"), "dd/MM/yyyy")) \
        .withColumn("checkin_date", to_date(col("checkin_date"), "dd/MM/yyyy")) \
        .withColumn("checkout_date", to_date(col("checkout_date"), "dd/MM/yyyy"))
    # Convert back to DynamicFrame
    dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")
    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Redshift.user_viewing
Redshiftuser_viewing_node1750243818781 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift_conn"}, transformation_ctx="Redshiftuser_viewing_node1750243818781")

# Script generated for node Redshift.apartment_attributes
Redshiftapartment_attributes_node1750244790593 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift_conn"}, transformation_ctx="Redshiftapartment_attributes_node1750244790593")

# Script generated for node Redshift.apartments
Redshiftapartments_node1750245265895 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift_conn"}, transformation_ctx="Redshiftapartments_node1750245265895")

# Script generated for node Redshift.bookings
Redshiftbookings_node1750245729645 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift_conn"}, transformation_ctx="Redshiftbookings_node1750245729645")

# Script generated for node Drop Duplicates.userViewing
DropDuplicatesuserViewing_node1750243880355 =  DynamicFrame.fromDF(Redshiftuser_viewing_node1750243818781.toDF().dropDuplicates(), glueContext, "DropDuplicatesuserViewing_node1750243880355")

# Script generated for node Drop Duplicates
DropDuplicates_node1750244869550 =  DynamicFrame.fromDF(Redshiftapartment_attributes_node1750244790593.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750244869550")

# Script generated for node Drop Duplicates.apartment
DropDuplicatesapartment_node1750245433493 =  DynamicFrame.fromDF(Redshiftapartments_node1750245265895.toDF().dropDuplicates(), glueContext, "DropDuplicatesapartment_node1750245433493")

# Script generated for node Drop Duplicates
DropDuplicates_node1750245792945 =  DynamicFrame.fromDF(Redshiftbookings_node1750245729645.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1750245792945")

# Script generated for node Custom Transform
CustomTransform_node1750243951319 = TransformUserViewing(glueContext, DynamicFrameCollection({"DropDuplicatesuserViewing_node1750243880355": DropDuplicatesuserViewing_node1750243880355}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750244940199 = TransformApartmentAttributes(glueContext, DynamicFrameCollection({"DropDuplicates_node1750244869550": DropDuplicates_node1750244869550}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750245471783 = TransformApartments(glueContext, DynamicFrameCollection({"DropDuplicatesapartment_node1750245433493": DropDuplicatesapartment_node1750245433493}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750245820351 = TransformBookings(glueContext, DynamicFrameCollection({"DropDuplicates_node1750245792945": DropDuplicates_node1750245792945}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1750244693735 = SelectFromCollection.apply(dfc=CustomTransform_node1750243951319, key=list(CustomTransform_node1750243951319.keys())[0], transformation_ctx="SelectFromCollection_node1750244693735")

# Script generated for node Select From Collection
SelectFromCollection_node1750245037080 = SelectFromCollection.apply(dfc=CustomTransform_node1750244940199, key=list(CustomTransform_node1750244940199.keys())[0], transformation_ctx="SelectFromCollection_node1750245037080")

# Script generated for node Select From Collection
SelectFromCollection_node1750245604710 = SelectFromCollection.apply(dfc=CustomTransform_node1750245471783, key=list(CustomTransform_node1750245471783.keys())[0], transformation_ctx="SelectFromCollection_node1750245604710")

# Script generated for node Select From Collection
SelectFromCollection_node1750245909485 = SelectFromCollection.apply(dfc=CustomTransform_node1750245820351, key=list(CustomTransform_node1750245820351.keys())[0], transformation_ctx="SelectFromCollection_node1750245909485")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750244713519 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750244693735, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.user_viewing", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750244713519")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750245107746 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750245037080, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartment_attributes", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes (id VARCHAR, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DECIMAL, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DECIMAL, longitude DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750245107746")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750245651238 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750245604710, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.apartments", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DECIMAL, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp DATE);"}, transformation_ctx="AmazonRedshift_node1750245651238")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750246004865 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750245909485, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated.bookings", "connectionName": "Redshift_conn", "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings (booking_id VARCHAR, user_id VARCHAR, apartment_id VARCHAR, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DECIMAL, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750246004865")

job.commit()
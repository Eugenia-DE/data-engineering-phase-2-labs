import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection, DynamicFrame
from pyspark.sql.functions import col, when, to_date, lower, trim
from pyspark.sql.types import (
    BooleanType, LongType, StringType, IntegerType, DecimalType, DateType, StructType, StructField
)
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def MyTransform(glueContext, dfc, target_layer='curated') -> DynamicFrameCollection:
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()
    columns = df.columns

    date_fields = ["booking_date", "checkin_date", "checkout_date", "viewed_at"]
    for field in date_fields:
        if field in columns and target_layer == 'curated':
            df = df.withColumn(field, to_date(col(field), "dd/MM/yyyy"))

    if target_layer == 'curated':
        bool_fields = ["is_wishlisted", "has_photo", "pets_allowed"]
        for field in bool_fields:
            if field in columns:
                df = df.withColumn(
                    field,
                    when(col(field) == "TRUE", True)
                    .when(col(field) == "FALSE", False)
                    .otherwise(None)
                    .cast(BooleanType())
                )

    string_fields = ["category", "amenities", "price_type", "cityname", "state"]
    for field in string_fields:
        if field in columns:
            df = df.withColumn(field, lower(trim(col(field))))

    dyf_transformed = DynamicFrame.fromDF(df, glueContext, "dyf_transformed")
    return DynamicFrameCollection({"CustomTransform": dyf_transformed}, glueContext)

def enforce_schema_and_clean(df, schema, primary_keys):
    # Cast columns to correct types
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        else:
            df = df.withColumn(field.name, col(field.name))

    # Drop rows with nulls in primary key columns
    if primary_keys:
        df = df.na.drop(subset=primary_keys)
    return df

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schemas and primary keys
schemas = {
    "apartments": {
        "schema": StructType([
            StructField("id", LongType(), True),
            StructField("title", StringType(), True),
            StructField("source", StringType(), True),
            StructField("price", DecimalType(10,2), True),
            StructField("currency", StringType(), True),
            StructField("listing_created_on", DateType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("last_modified_timestamp", DateType(), True)
        ]),
        "primary_keys": ["id"]
    },
    "apartment_attributes": {
        "schema": StructType([
            StructField("id", LongType(), True),
            StructField("category", StringType(), True),
            StructField("body", StringType(), True),
            StructField("amenities", StringType(), True),
            StructField("bathrooms", IntegerType(), True),
            StructField("bedrooms", IntegerType(), True),
            StructField("fee", DecimalType(10,2), True),
            StructField("has_photo", BooleanType(), True),
            StructField("pets_allowed", BooleanType(), True),
            StructField("price_display", StringType(), True),
            StructField("price_type", StringType(), True),
            StructField("square_feet", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("cityname", StringType(), True),
            StructField("state", StringType(), True),
            StructField("latitude", DecimalType(9,6), True),
            StructField("longitude", DecimalType(9,6), True)
        ]),
        "primary_keys": ["id"]
    },
    "bookings": {
        "schema": StructType([
            StructField("booking_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("apartment_id", LongType(), True),
            StructField("booking_date", DateType(), True),
            StructField("checkin_date", DateType(), True),
            StructField("checkout_date", DateType(), True),
            StructField("total_price", DecimalType(10,2), True),
            StructField("currency", StringType(), True),
            StructField("booking_status", StringType(), True)
        ]),
        "primary_keys": ["booking_id"]
    },
    "user_viewing": {
        "schema": StructType([
            StructField("user_id", LongType(), True),
            StructField("apartment_id", LongType(), True),
            StructField("viewed_at", DateType(), True),
            StructField("is_wishlisted", BooleanType(), True),
            StructField("call_to_action", StringType(), True)
        ]),
        "primary_keys": ["user_id", "apartment_id", "viewed_at"]
    }
}

# Load raw data
def load_raw_table(table_name):
    return glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/",
            "useConnectionProperties": "true",
            "dbtable": f"raw_layer.{table_name}",
            "connectionName": "Redshift_conn"
        },
        transformation_ctx=f"{table_name}_raw"
    )

tables = ["apartments", "apartment_attributes", "bookings", "user_viewing"]
raw_dfs = {t: load_raw_table(t) for t in tables}

# Transform all tables
curated_dfc = {}
for t in tables:
    curated_dfc[t] = MyTransform(glueContext, DynamicFrameCollection({f"{t}_raw": raw_dfs[t]}, glueContext), target_layer='curated')

# Select from collections
curated_sel = {}
for t in tables:
    curated_sel[t] = SelectFromCollection.apply(dfc=curated_dfc[t], key=list(curated_dfc[t].keys())[0])

# Enforce schema, clean, and prepare DynamicFrames for write
prepared_dyf = {}
for t in tables:
    df = curated_sel[t].toDF()
    df = enforce_schema_and_clean(df, schemas[t]["schema"], schemas[t]["primary_keys"])
    logger.info(f"{t} schema before write:")
    df.printSchema()
    df.show(5)
    prepared_dyf[t] = DynamicFrame.fromDF(df, glueContext, f"dyf_{t}")

# Write each curated table with error handling
def write_table(dyf, table_name, create_table_sql):
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options={
                "redshiftTmpDir": "s3://aws-glue-assets-371439860588-eu-west-1/temporary/",
                "useConnectionProperties": "true",
                "dbtable": f"curated.{table_name}",
                "connectionName": "Redshift_conn",
                "preactions": create_table_sql
            },
            transformation_ctx=f"{table_name}_curated_write"
        )
        logger.info(f"Successfully wrote table curated.{table_name}")
    except Exception as e:
        logger.error(f"Error writing table curated.{table_name}: {str(e)}")
        raise

# Preactions SQL statements
preactions_sql = {
    "apartments": """
        CREATE TABLE IF NOT EXISTS curated.apartments (
            id BIGINT PRIMARY KEY,
            title VARCHAR(512),
            source VARCHAR(255),
            price DECIMAL(10,2),
            currency VARCHAR(10),
            listing_created_on DATE,
            is_active BOOLEAN,
            last_modified_timestamp DATE
        );
    """,
    "apartment_attributes": """
        CREATE TABLE IF NOT EXISTS curated.apartment_attributes (
            id BIGINT PRIMARY KEY,
            category VARCHAR(255),
            body VARCHAR(MAX),
            amenities VARCHAR(MAX),
            bathrooms INT,
            bedrooms INT,
            fee DECIMAL(10,2),
            has_photo BOOLEAN,
            pets_allowed BOOLEAN,
            price_display VARCHAR(255),
            price_type VARCHAR(255),
            square_feet INT,
            address VARCHAR(512),
            cityname VARCHAR(255),
            state VARCHAR(255),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6)
        );
    """,
    "bookings": """
        CREATE TABLE IF NOT EXISTS curated.bookings (
            booking_id BIGINT PRIMARY KEY,
            user_id BIGINT,
            apartment_id BIGINT,
            booking_date DATE,
            checkin_date DATE,
            checkout_date DATE,
            total_price DECIMAL(10,2),
            currency VARCHAR(10),
            booking_status VARCHAR(255)
        );
    """,
    "user_viewing": """
        CREATE TABLE IF NOT EXISTS curated.user_viewing (
            user_id BIGINT,
            apartment_id BIGINT,
            viewed_at DATE,
            is_wishlisted BOOLEAN,
            call_to_action VARCHAR(255),
            PRIMARY KEY (user_id, apartment_id, viewed_at)
        );
    """
}

# Write all tables
for t in tables:
    write_table(prepared_dyf[t], t, preactions_sql[t])

job.commit()
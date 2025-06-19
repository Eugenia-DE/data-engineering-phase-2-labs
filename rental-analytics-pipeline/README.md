# AWS Data Pipeline for Rental Analytics

## Project Overview

This project implements a robust, scalable, and serverless batch data processing pipeline on Amazon Web Services (AWS). The primary goal is to extract rental listing data from an **AWS Aurora MySQL** operational database, transform it, and load it into a multi-layered data warehouse within **Amazon Redshift Serverless** for analytical purposes. The entire ETL (Extract, Transform, Load) workflow is orchestrated using **AWS Step Functions**.

This pipeline establishes a modern data architecture designed for flexibility, cost-efficiency, and scalability, allowing for deep analytical insights into rental market trends.

## Key Features & Components

* **Source Data:** Rental listing data (e.g., apartments, bookings, user viewings) from **AWS Aurora MySQL Serverless v2**.
* **ETL Orchestration:** End-to-end pipeline workflow managed by **AWS Step Functions**.
* **Data Processing:** Serverless ETL jobs built with **AWS Glue** (using Visual ETL).
* **Staging Layer:** **Amazon S3** for temporary storage of raw extracted data.
* **Data Warehousing:** **Amazon Redshift Serverless** as the analytical data warehouse, implementing a multi-layered architecture:
    * **Raw Layer:** Stores data directly loaded from S3, mirroring source schemas.
    * **Curated Layer:** Contains cleaned, transformed, and integrated data, suitable for general analysis.
    * **Presentation Layer:** Holds highly aggregated and optimized data for specific reporting and BI tools.
* **Secure Networking:** All core resources operate within a **Virtual Private Cloud (VPC)**, leveraging **Security Groups** and **VPC Endpoints** (S3 Gateway, STS Interface, Secrets Manager Interface) for private and secure communication.
* **Identity & Access Management:** **AWS IAM** roles and policies ensure granular permissions for all services.

## Architecture

The pipeline follows a typical batch processing pattern, moving data from an OLTP source to an OLAP data warehouse.


### Data Flow Overview

1.  **Extraction (Job1):** AWS Glue ETL Job `Job1` (Aurora to S3 Raw Extraction) reads data from **Aurora MySQL** and writes the raw data files to **Amazon S3**.
2.  **Loading Raw (Job2):** AWS Glue ETL Job `Job2` (S3 Raw to Redshift Raw Load) reads the raw data from **S3** and loads it into the **Raw Layer** in **Redshift Serverless**.
3.  **Curated Transformation (Job3):** AWS Glue ETL Job `Job3` (Redshift Raw to Curated Transform) reads data from the **Redshift Raw Layer**, performs cleaning and transformations, and loads it into the **Redshift Curated Layer**.
4.  **Presentation Transformation (Job4):** AWS Glue ETL Job `Job4` (Redshift Curated to Presentation Transform) further processes data from the **Redshift Curated Layer** into the **Redshift Presentation Layer**, optimized for consumption.
5.  **Orchestration:** **AWS Step Functions** ensures these Glue jobs run sequentially and handles workflow management.

## Setup & Deployment

To deploy and run this data pipeline in your AWS account, follow these steps:

### 1. AWS Account & IAM Configuration

Ensure you have an active AWS account and an IAM user with sufficient permissions (e.g., `AdministratorAccess` for initial setup in a development environment).

* **IAM Role for Glue (`AWSGlueAuroraS3Role`):** This critical role allows Glue to interact with all necessary services.
    * Details on required policies and ARN: [IAM Role Setup](setup/iam_roles.md)

### 2. Network Infrastructure (VPC)

All core services are deployed within a Virtual Private Cloud (VPC) for secure, private communication.

* **VPC and Private Subnets:** Identify or create a VPC with at least two private subnets in `eu-west-1`.
    * VPC and Subnet details: [VPC Networking](setup/vpc_networking.md)
* **Security Groups:**
    * `aurora-sg`: For Aurora MySQL.
    * `glue-connection-sg`: For AWS Glue's network interfaces.
    * `redshift-serverless-sg`: For Redshift Serverless.
    * Ensure proper inbound/outbound rules are configured to allow communication between Glue, Aurora, and Redshift.
* **VPC Endpoints:** Essential for private access to AWS services from within your VPC.
    * **S3 Gateway Endpoint:** For Glue to access S3 privately.
    * **STS Interface Endpoint:** For Glue's internal credential management.
    * **Secrets Manager Interface Endpoint:** For secure secrets access (even if not directly used for DB creds, Glue may have internal dependencies).
    * Configuration details: [VPC Endpoints Setup](setup/vpc_endpoints.md)

### 3. Data Sources & Targets

* **AWS Aurora MySQL Serverless v2 (`rentaldb`):**
    * Deploy your Aurora cluster in your private subnets, associated with `aurora-sg`.
    * Ensure initial rental data is loaded into `rentaldb` (e.g., using `.sql` scripts or data files).
    * Database creation instructions: [Database Creation](setup/database_creation.md)
* **Amazon S3 Bucket:**
    * Create a dedicated S3 bucket (e.g., `yourname-rental-data-pipeline-<suffix>`) in `eu-west-1` for storing raw extracted data.
* **Amazon Redshift Serverless:**
    * Create a Redshift Serverless Workgroup in your private subnets, ensuring its associated security group allows inbound traffic from `glue-connection-sg`.
    * Define the schema for your **Raw Layer** tables in Redshift.
    * Database creation instructions: [Database Creation](setup/database_creation.md)
    * Redshift DDL for layers: [Redshift DDL](redshift/ddl/)

### 4. AWS Glue Configuration

* **Glue Connections:**
    * Create an `aurora-mysql-connection` linking Glue to your Aurora database.
    * Create a `redshift-serverless-connection` linking Glue to your Redshift Serverless instance.
    * Connection details: [Glue Connections](glue/connections.md)
* **Glue ETL Jobs:**
    * **`Job1` (Aurora to S3 Raw Extraction):** This job (likely created via Glue Visual ETL) extracts data.
    * **`Job2` (S3 Raw to Redshift Raw Load):** Loads data from S3 into Redshift's Raw Layer.
    * **`Job3` (Redshift Raw to Curated Transform):** Transforms data from Raw to Curated.
    * **`Job4` (Redshift Curated to Presentation Transform):** Transforms data from Curated to Presentation.
    * Refer to the `glue/jobs/` directory for detailed job configurations and PySpark scripts (if applicable).

### 5. AWS Step Functions Orchestration

* **Define Workflow:** Create a Step Functions State Machine that orchestrates the sequential execution of your Glue ETL jobs (`Job1` -> `Job2` -> `Job3` -> `Job4`).
    * Your Step Functions definition: [Pipeline Workflow ASL](orchestration/pipeline_workflow.asl.json)
    * Visual diagram of the workflow: [Step Functions Diagram](orchestration/step_functions_diagram.png)
    * Explanation of orchestration logic: [Orchestration Logic](orchestration/orchestration_logic.md)

## Usage

Once all setup steps are complete and your Glue jobs are configured:

1.  **Manual Trigger:** You can manually start the Step Functions State Machine from the AWS Console.
2.  **Automated Scheduling (Future):** Integrate the Step Functions workflow with Amazon EventBridge for scheduled runs (e.g., daily).

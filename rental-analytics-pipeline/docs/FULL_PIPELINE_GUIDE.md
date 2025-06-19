# Full Data Pipeline Setup and Run Guide

This document provides a comprehensive, step-by-step guide to setting up the AWS environment and executing the batch data processing pipeline for rental listing data.

---

## Table of Contents

1.  [Project Overview](#1-project-overview)
2.  [AWS Setup Prerequisites](#2-aws-setup-prerequisites)
    * 2.1. AWS Account & IAM User Setup
    * 2.2. AWS IAM Role for Glue (`AWSGlueAuroraS3Role`)
    * 2.3. AWS VPC & Networking Configuration
    * 2.4. Amazon S3 Bucket
    * 2.5. AWS Aurora MySQL Database Setup
    * 2.6. Amazon Redshift Serverless Setup
3.  [AWS Glue Configuration](#3-aws-glue-configuration)
    * 3.1. Glue Connection to Aurora MySQL (`aurora-mysql-connection`)
    * 3.2. Glue Connection to Redshift Serverless (`redshift-serverless-connection`)
4.  [Pipeline Execution (AWS Glue ETL Jobs & Step Functions)](#4-pipeline-execution-aws-glue-etl-jobs--step-functions)
    * 4.1. Understanding the Glue ETL Jobs
    * 4.2. Creating AWS Glue ETL Jobs
    * 4.3. Creating the AWS Step Functions State Machine
    * 4.4. Running the Pipeline
    * 4.5. Monitoring Pipeline Execution
5.  [Post-Execution Verification](#5-post-execution-verification)
6.  [Troubleshooting Tips](#6-troubleshooting-tips)

---

## 1. Project Overview

This project implements a robust batch data processing pipeline on AWS. The core objective is to extract rental listing data from **AWS Aurora MySQL**, perform ETL (Extract, Transform, Load) operations using **AWS Glue**, and load the processed data into **Amazon Redshift Serverless** for analytical purposes. The pipeline adheres to a multi-layered architecture within Redshift (Raw, Curated, Presentation) and is orchestrated using **AWS Step Functions**.

**Key Components:**

* **Data Source:** AWS Aurora MySQL (`rentaldb`)
* **ETL Service:** AWS Glue
* **Intermediate Storage:** Amazon S3
* **Data Warehouse:** Amazon Redshift Serverless (with Raw, Curated, Presentation layers)
* **Orchestration:** AWS Step Functions
* **Networking & Security:** Amazon VPC, Security Groups, VPC Endpoints, IAM Roles

For a visual representation of the architecture, please refer to the diagrams in the `architecture/` directory of this repository.

## 2. AWS Setup Prerequisites

This section details the initial setup of core AWS services required for the pipeline. All resources should be created in the **`eu-west-1` (Ireland) region**.

### 2.1. AWS Account & IAM User Setup

1.  **AWS Account:** Ensure you have an active AWS account.
2.  **IAM User:**
    * Log in to the AWS Management Console with an IAM User that has `AdministratorAccess` (for initial setup simplicity; permissions can be refined later).
    * Verify your default region is set to `eu-west-1`.

### 2.2. AWS IAM Role for Glue (`AWSGlueAuroraS3Role`)

This IAM Role grants AWS Glue the necessary permissions to interact with other AWS services required by the pipeline.

1.  Navigate to **IAM** service in the AWS Console.
2.  Go to **"Roles"** and click **"Create role"**.
3.  **Select trusted entity:** Choose `AWS service`.
4.  **Use case:** Select `Glue`.
5.  Click **"Next"**.
6.  **Add permissions:** Attach the following AWS managed policies:
    * `AWSGlueServiceRole`
    * `AmazonS3FullAccess` (For simplicity; can be scoped down to specific buckets later)
    * `AmazonRDSDataFullAccess`
    * `AmazonRedshiftFullAccess` (For simplicity; can be scoped down to specific resources/actions later)
    * `SecretsManagerReadWrite`
    * `AWSSecurityTokenServiceFullAccess`
7.  Click **"Next"**.
8.  **Role details:**
    * **Role name:** `AWSGlueAuroraS3Role`
    * **Description:** `IAM role for AWS Glue jobs to access Aurora, S3, and Redshift for rental data pipeline.`
9.  Click **"Create role"**.
10. **Note:** Make a note of the ARN for this role.

### 2.3. AWS VPC & Networking Configuration

This sets up the private network environment for your databases and Glue.

1.  **Identify/Create a VPC:**
    * Navigate to the **VPC** service.
    * Use your default VPC in `eu-west-1` or create a new one.
    * **Note down its VPC ID** (e.g., `vpc-09a288bf40b1947b8`).
2.  **Private Subnets:**
    * Ensure you have at least **two private subnets** within your chosen VPC, ideally in different Availability Zones within `eu-west-1`. These will host your databases and Glue's ENIs.
    * **Note down their Subnet IDs** (e.g., `subnet-09426f12406f098be`, `subnet-abcdef1234567890a`).
3.  **Security Groups:**
    * **`aurora-sg`:**
        * Create a new security group.
        * **Name:** `aurora-sg`
        * **Description:** `Security Group for Aurora DB instance.`
        * **VPC:** Select your chosen VPC.
        * **Inbound Rules:** None initially. (Will be updated in Section 2.5).
        * **Outbound Rules:** All traffic allowed (default).
    * **`glue-connection-sg`:**
        * Create a new security group.
        * **Name:** `glue-connection-sg`
        * **Description:** `Security Group for AWS Glue connections to VPC resources.`
        * **VPC:** Select your chosen VPC.
        * **Inbound Rules:** None (Glue will initiate outbound connections).
        * **Outbound Rules:** Add a rule: `Type: All traffic`, `Destination: 0.0.0.0/0`. This allows Glue to initiate connections to any IP address.
4.  **VPC Endpoints (Under VPC -> PrivateLink -> Endpoints):**
    These endpoints allow private communication from your VPC to essential AWS services, crucial for Glue operating in private subnets.
    * **Amazon S3 Gateway Endpoint:**
        * Click **"Create endpoint"**.
        * **Service category:** `AWS services`.
        * **Service name:** Search for `s3` and select `com.amazonaws.eu-west-1.s3` (Type: `Gateway`).
        * **VPC:** Select your chosen VPC.
        * **Route tables:** Select the route tables associated with your private subnets (this automatically creates a route for S3 traffic).
        * Click **"Create endpoint"**.
    * **AWS Security Token Service (STS) Interface Endpoint:**
        * Click **"Create endpoint"**.
        * **Service category:** `AWS services`.
        * **Service name:** Search for `sts` and select `com.amazonaws.eu-west-1.sts` (Type: `Interface`).
        * **VPC:** Select your chosen VPC.
        * **Subnets:** Select your private subnets (where Glue ENIs will be created).
        * **Security groups:** Select your `glue-connection-sg`.
        * Click **"Create endpoint"**.
    * **AWS Secrets Manager Interface Endpoint:**
        * Click **"Create endpoint"**.
        * **Service category:** `AWS services`.
        * **Service name:** Search for `secretsmanager` and select `com.amazonaws.eu-west-1.secretsmanager` (Type: `Interface`).
        * **VPC:** Select your chosen VPC.
        * **Subnets:** Select your private subnets.
        * **Security groups:** Select your `glue-connection-sg`.
        * Click **"Create endpoint"**.

### 2.4. Amazon S3 Bucket

This bucket will serve as the intermediate storage for your extracted raw data.

1.  Navigate to **S3** service.
2.  Click **"Create bucket"**.
3.  **Bucket name:** Choose a globally unique name (e.g., `yourname-rental-data-pipeline-eu-west-1`).
4.  **AWS Region:** Select `eu-west-1`.
5.  Leave other settings as default (ensure "Block all public access" is enabled for security).
6.  Click **"Create bucket"**.
7.  **(Optional) Upload Initial Data:** If you have initial CSV files for loading into Aurora (e.g., `apartments.csv`, `bookings.csv`), you can upload them to a dedicated folder in this S3 bucket now.

### 2.5. AWS Aurora MySQL Database Setup

This is your source OLTP database.

1.  Navigate to **RDS** service.
2.  Go to **"Databases"** and click **"Create database"**.
3.  **Choose a database creation method:** `Standard create`.
4.  **Engine options:**
    * **Engine type:** `Amazon Aurora`.
    * **Edition:** `Amazon Aurora MySQL-Compatible Edition`.
    * **Version:** Choose `MySQL 8.0` compatible.
5.  **Database features:** Select `Serverless v2`.
6.  **DB cluster identifier:** `rental-db-aurora`.
7.  **Master username:** Choose a strong username (e.g., `admin`).
8.  **Master password:** Choose a strong password and **note it down securely**.
9.  **VPC:** Select your chosen VPC.
10. **DB subnet group:** Create a new one and select your **private subnets**.
11. **Public access:** Select `No`.
12. **VPC security group:** Choose `Existing VPC security groups` and select your `aurora-sg`.
13. **Database name:** `rentaldb`.
14. Leave other settings as default.
15. Click **"Create database"**.
16. **Wait** for the cluster to be in `Available` status.
17. **Note down the Cluster Endpoint** (e.g., `rental-db-aurora.cluster-xxxx.eu-west-1.rds.amazonaws.com`).
18. **Update `aurora-sg` inbound rules:**
    * Go to **EC2** -> **"Security Groups"**.
    * Select `aurora-sg`.
    * Go to **"Inbound rules"** tab and click **"Edit inbound rules"**.
    * **Add rule:** `Type: MYSQL/Aurora`, `Port range: 3306`, **`Source: Custom`** and enter the **ID of your `glue-connection-sg`** (e.g., `sg-080125150cc5bfd8f`).
    * Click **"Save rules"**.
19. **Load Initial Data into Aurora:** Connect to your Aurora `rentaldb` using a MySQL client (e.g., MySQL Workbench, DBeaver) from a host that has network access to your VPC (e.g., an EC2 instance in the same VPC, or via a bastion host/VPN). Load your rental listing data (e.g., `apartments`, `bookings`, `user_viewing`, `apartment_attributes`) into the `rentaldb` database. You will need to create the table schemas first (DDL for these tables can be found in `aurora/ddl/rentaldb_schema.sql` in the repository).

### 2.6. Amazon Redshift Serverless Setup

This is your target analytical data warehouse.

1.  Navigate to **Redshift** service.
2.  Go to **"Serverless"** -> **"Workgroups"**.
3.  Click **"Create workgroup"**.
4.  **Workgroup name:** Choose a descriptive name (e.g., `rental-data-warehouse`).
5.  **Namespace name:** A namespace will be automatically created or you can create a new one. (e.g., `default`).
6.  **Administrator user name:** Choose a strong username (e.g., `admin`).
7.  **Administrator password:** Choose a strong password and **note it down securely**.
8.  **VPC:** Select your chosen VPC.
9.  **Subnets:** Select your private subnets.
10. **VPC security groups:** By default, Redshift Serverless creates a security group for your workgroup. **Note down the ID of this security group** (e.g., `sg-redshiftserverless-xxxx`).
11. Leave other settings as default.
12. Click **"Create workgroup"**.
13. **Wait** for the workgroup to be in `Available` status.
14. **Note down the Workgroup Endpoint** (e.g., `workgroup-name.123456789012.eu-west-1.redshift-serverless.amazonaws.com:5439/dev`).
15. **Update Redshift Serverless Workgroup Security Group inbound rules:**
    * Go to **EC2** -> **"Security Groups"**.
    * Select the security group associated with your Redshift Serverless workgroup (the one you noted down).
    * Go to **"Inbound rules"** tab and click **"Edit inbound rules"**.
    * **Add rule:** `Type: Redshift` (or `Custom TCP`), `Port range: 5439`, **`Source: Custom`** and enter the **ID of your `glue-connection-sg`** (e.g., `sg-080125150cc5bfd8f`).
    * Click **"Save rules"**.
16. **(Optional) Create Redshift Raw Layer Schemas:** Connect to your Redshift Serverless endpoint using a SQL client (e.g., DBeaver) and create the tables for your `Raw Layer` as defined in `redshift/ddl/raw_layer_ddl.sql` in the repository.

## 3. AWS Glue Configuration

This section details the setup of Glue connections, which allow Glue to communicate with your databases.

### 3.1. Glue Connection to Aurora MySQL (`aurora-mysql-connection`)

1.  Navigate to **AWS Glue** service.
2.  Go to **"Data Catalog"** -> **"Connections"**.
3.  Click **"Create connection"**.
4.  **Connection properties:**
    * **Connection name:** `aurora-mysql-connection`
    * **Connection type:** `JDBC`
    * Click **"Next"**.
5.  **Enter connection details:**
    * **JDBC URL:** `jdbc:mysql://<YOUR_AURORA_CLUSTER_ENDPOINT>:3306/rentaldb` (Replace `<YOUR_AURORA_CLUSTER_ENDPOINT>` with the actual endpoint noted in Section 2.5).
    * **Username:** Your Aurora master username.
    * **Password:** Your Aurora master password.
    * **VPC:** Select your chosen VPC.
    * **Subnet:** Select one of your private subnets.
    * **Security groups:** Select your `glue-connection-sg`.
6.  Click **"Next"**, then **"Create connection"**.
7.  **Test Connection:** Select `aurora-mysql-connection`, click **"Test connection"**, and choose `AWSGlueAuroraS3Role` as the IAM role. It should display "Successfully connected."

### 3.2. Glue Connection to Redshift Serverless (`redshift-serverless-connection`)

1.  Navigate to **AWS Glue** service.
2.  Go to **"Data Catalog"** -> **"Connections"**.
3.  Click **"Create connection"**.
4.  **Connection properties:**
    * **Connection name:** `redshift-serverless-connection`
    * **Connection type:** `JDBC`
    * Click **"Next"**.
5.  **Enter connection details:**
    * **JDBC URL:** `jdbc:redshift://<YOUR_REDSHIFT_SERVERLESS_ENDPOINT>` (Replace `<YOUR_REDSHIFT_SERVERLESS_ENDPOINT>` with the actual endpoint noted in Section 2.6, including port and database name like `:5439/dev`).
    * **Username:** Your Redshift Serverless administrator username.
    * **Password:** Your Redshift Serverless administrator password.
    * **VPC:** Select your chosen VPC.
    * **Subnet:** Select one of your private subnets.
    * **Security groups:** Select your `glue-connection-sg`.
6.  Click **"Next"**, then **"Create connection"**.
7.  **Test Connection:** Select `redshift-serverless-connection`, click **"Test connection"**, and choose `AWSGlueAuroraS3Role` as the IAM role. It should display "Successfully connected."

## 4. Pipeline Execution (AWS Glue ETL Jobs & Step Functions)

This section details the creation of the Glue ETL jobs and the Step Functions state machine to orchestrate them.

### 4.1. Understanding the Glue ETL Jobs

Your pipeline consists of four sequential Glue ETL jobs, orchestrated by Step Functions:

* **Job1: Aurora to S3 Raw Extraction**
    * **Purpose:** Extracts raw data from Aurora MySQL and writes it to your S3 bucket.
    * **Input:** Aurora MySQL (`rentaldb`)
    * **Output:** S3 (`s3://yourname-rental-data-pipeline-eu-west-1/raw_extracted_data/`)
    * **Code/Configuration:** Refer to `glue/jobs/aurora_to_s3_raw/`
* **Job2: S3 Raw to Redshift Raw Load**
    * **Purpose:** Loads the raw data from S3 into the `Raw Layer` tables in Redshift Serverless.
    * **Input:** S3 (`s3://yourname-rental-data-pipeline-eu-west-1/raw_extracted_data/`)
    * **Output:** Redshift Serverless (`Raw Layer`)
    * **Code/Configuration:** Refer to `glue/jobs/s3_raw_to_redshift_raw/`
* **Job3: Redshift Raw to Curated Transform**
    * **Purpose:** Transforms and cleans data from the `Raw Layer` and loads it into the `Curated Layer` in Redshift.
    * **Input:** Redshift Serverless (`Raw Layer`)
    * **Output:** Redshift Serverless (`Curated Layer`)
    * **Code/Configuration:** Refer to `glue/jobs/redshift_raw_to_curated/`
* **Job4: Redshift Curated to Presentation Transform**
    * **Purpose:** Aggregates and optimizes data from the `Curated Layer` for the `Presentation Layer` in Redshift.
    * **Input:** Redshift Serverless (`Curated Layer`)
    * **Output:** Redshift Serverless (`Presentation Layer`)
    * **Code/Configuration:** Refer to `glue/jobs/redshift_curated_to_presentation/`

### 4.2. Creating AWS Glue ETL Jobs

For each job (Job1, Job2, Job3, Job4), you will create an AWS Glue ETL job definition.

1.  Navigate to **AWS Glue** service.
2.  Go to **"ETL"** -> **"Jobs"**.
3.  Click **"Create job"**.
4.  **Job details (for each job):**
    * **Name:** E.g., `pipeline-job-aurora-to-s3-raw`, `pipeline-job-s3-to-redshift-raw`, etc. (Use descriptive names matching Job1, Job2, etc. roles).
    * **IAM role:** Select `AWSGlueAuroraS3Role`.
    * **Type:**
        * For `Job1 (Aurora to S3 Raw)`, you can use `Visual ETL` (as previously done) or `Spark script editor`. If using Visual ETL, configure the source as your Aurora connection and the target as your S3 bucket.
        * For `Job2, Job3, Job4`, it's recommended to use `Spark script editor` (Python).
            * **Script file:** Select `Create a new script`.
            * **S3 path for script:** Use default or your preferred S3 bucket.
            * **Temporary directory:** Use default or your preferred S3 bucket.
    * **Glue version:** Choose the latest recommended (e.g., `Glue 4.0`).
    * **Language:** `Python`.
    * **Job timeout:** `30 minutes` (or more, adjust as needed).
    * **Worker type:** `G.1X` (recommended for Spark jobs).
    * **Number of workers:** `2` (G.1X) or `10` (Standard) - adjust based on data volume.
    * **Connections:** For each job, select the relevant Glue connections:
        * `Job1`: `aurora-mysql-connection`
        * `Job2`: `redshift-serverless-connection`
        * `Job3`: `redshift-serverless-connection`
        * `Job4`: `redshift-serverless-connection`
    * Click **"Next"** and then **"Save job and edit script"**.
5.  **Develop/Upload Scripts:**
    * For `Job1` (if using Visual ETL), configure it through the UI.
    * For `Job2`, `Job3`, `Job4`: Write or copy the PySpark script content into the editor based on the examples in `glue/jobs/` directory in this repository. Save the script.

### 4.3. Creating the AWS Step Functions State Machine

This state machine will define and execute the sequence of your Glue jobs.

1.  Navigate to **AWS Step Functions** service.
2.  Go to **"State machines"** and click **"Create state machine"**.
3.  **Choose authoring method:** Select `Write your workflow in code` (unless you prefer to use Workflow Studio).
4.  **Type:** `Standard`.
5.  **Definition:** Paste the Amazon States Language (ASL) definition from `orchestration/pipeline_workflow.asl.json` in your repository. **IMPORTANT:** Update the `Resource` ARNs for each `Task` state (Job1, Job2, Job3, Job4) to point to the actual ARNs of the Glue jobs you created in Section 4.2.
    * Example ASL snippet:
        ```json
        {
          "Comment": "Rental Data Pipeline Orchestration",
          "StartAt": "RunJob1_ExtractAuroraToS3",
          "States": {
            "RunJob1_ExtractAuroraToS3": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "pipeline-job-aurora-to-s3-raw",
                "Arguments": {}
              },
              "End": true,
              "Next": "RunJob2_S3ToRedshiftRaw"
            },
            "RunJob2_S3ToRedshiftRaw": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "pipeline-job-s3-to-redshift-raw",
                "Arguments": {}
              },
              "End": true,
              "Next": "RunJob3_RedshiftRawToCurated"
            },
            "RunJob3_RedshiftRawToCurated": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "pipeline-job-redshift-raw-to-curated",
                "Arguments": {}
              },
              "End": true,
              "Next": "RunJob4_RedshiftCuratedToPresentation"
            },
            "RunJob4_RedshiftCuratedToPresentation": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "pipeline-job-redshift-curated-to-presentation",
                "Arguments": {}
              },
              "End": true
            }
          }
        }
        ```
        *(Note: The `End: true` on intermediate steps means it will stop there if no `Next` is defined. For a sequence, only the final step has `End: true`. I've corrected the example to chain them. Make sure your ASL chains them correctly using `Next` properties.)*
        *The correct chaining for sequential jobs looks like this (simplified):*
        ```json
        {
          "Comment": "Rental Data Pipeline Orchestration",
          "StartAt": "RunJob1_ExtractAuroraToS3",
          "States": {
            "RunJob1_ExtractAuroraToS3": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "pipeline-job-aurora-to-s3-raw" },
              "Next": "RunJob2_S3ToRedshiftRaw"
            },
            "RunJob2_S3ToRedshiftRaw": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "pipeline-job-s3-to-redshift-raw" },
              "Next": "RunJob3_RedshiftRawToCurated"
            },
            "RunJob3_RedshiftRawToCurated": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "pipeline-job-redshift-raw-to-curated" },
              "Next": "RunJob4_RedshiftCuratedToPresentation"
            },
            "RunJob4_RedshiftCuratedToPresentation": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "pipeline-job-redshift-curated-to-presentation" },
              "End": true
            }
          }
        }
        ```
6.  Click **"Next"**.
7.  **Specify state machine settings:**
    * **State machine name:** `rental-data-pipeline-orchestrator`
    * **Permissions:** Choose an `Existing role` and select your `AWSGlueAuroraS3Role` (ensure this role has `states:StartExecution` permissions for Glue jobs and `iam:PassRole` permissions for the Glue role).
8.  Click **"Create state machine"**.

### 4.4. Running the Pipeline

Once the Step Functions state machine is created, you can execute your entire data pipeline with a single click.

1.  Navigate to **AWS Step Functions** service.
2.  Go to **"State machines"**.
3.  Select your `rental-data-pipeline-orchestrator` state machine.
4.  Click **"Start execution"**.
5.  You can leave the input as default (`{}`).
6.  Click **"Start execution"** again.

### 4.5. Monitoring Pipeline Execution

Monitor the progress and status of your pipeline execution in Step Functions.

1.  After starting an execution, you will be redirected to the **Execution details** page.
2.  Observe the **Graph view** to see the progression of each step (Glue job).
3.  Check the **Step output** for details on each job's status.
4.  If a job fails, the Step Functions execution will stop at that step. You can examine the **Input**, **Output**, and **Error** tabs for details, and also click the link to the **Glue job run** to investigate its logs in CloudWatch.

## 5. Post-Execution Verification

After a successful pipeline run:

1.  **S3 Verification:**
    * Navigate to your S3 bucket (`yourname-rental-data-pipeline-eu-west-1`).
    * Verify that `raw_extracted_data/` prefix contains the extracted files from Aurora.
2.  **Redshift Verification:**
    * Connect to your Redshift Serverless endpoint using a SQL client.
    * Verify data exists in your `Raw Layer` tables.
    * Verify data exists in your `Curated Layer` tables.
    * Verify data exists in your `Presentation Layer` tables.
    * Run sample queries from `redshift/sql_queries/validation_queries.sql` to check data quality and completeness.

## 6. Troubleshooting Tips

* **"Connection creation failed during validating credentials. Please validate connection inputs and VPC connectivity to Security Token Service, Secrets Manager and REDSHIFT."**
    * **Solution:** Double-check Redshift Serverless username/password for typos. Ensure all **VPC Endpoints** (S3 Gateway, STS Interface, Secrets Manager Interface) are correctly set up in the *same VPC and subnets* where Glue is operating.
* **Glue Job Fails (Networking/Connectivity):**
    * **Solution:** Verify your Security Group rules:
        * `glue-connection-sg` outbound rule: Must allow `All traffic` to `0.0.0.0/0`.
        * `aurora-sg` inbound rule: Must allow `MySQL/Aurora (3306)` from `glue-connection-sg`.
        * Redshift Serverless SG inbound rule: Must allow `Redshift (5439)` from `glue-connection-sg`.
    * Ensure Glue Connection details (VPC, Subnet, Security Group) are correctly selected.
* **Glue Job Fails (Permissions):**
    * **Solution:** Review the `AWSGlueAuroraS3Role` IAM policy. Ensure it has permissions for S3 (`s3:GetObject`, `s3:PutObject`), RDS Data API (if used), Redshift (`redshift:DescribeClusters`, `redshift-serverless:*`), Glue actions, and access to KMS keys if S3 buckets are encrypted.
* **Glue Job Fails (Data/Script Error):**
    * **Solution:** Check the Glue job run logs in **CloudWatch Logs** for detailed error messages. This will pinpoint issues with data format, schema mismatches, or errors in your PySpark script.
* **Step Functions Execution Fails:**
    * **Solution:** Check the Step Functions **Graph view** to identify the failing step. Click on the step and review the **Error** and **Logs** tabs for the underlying Glue job failure. Ensure the Step Functions IAM role has `states:StartExecution` and `iam:PassRole` permissions for the Glue jobs.

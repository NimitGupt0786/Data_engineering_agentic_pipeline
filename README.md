**Overview**

**This project implements an automated, agent-driven data ingestion pipeline that loads files from Amazon S3 into Amazon Redshift Serverless using AWS Lambda, Bedrock (Claude 3 Sonnet), and Strands Agents.**

**The pipeline supports:**
Data quality validation & profiling
Schema inference & DDL generation
Redshift table creation
Full load & incremental load with SCD Type 2 handling
Automated archival of source files
All orchestration is performed dynamically at runtime using AI agents.


EXECUTION:-

**Agents Overview**

**1. Quality Assurance Agent**

**Purpose**
Ensures data quality before loading.

**Responsibilities**
Detect file metadata
Profile dataset
Execute data quality rules
Remove duplicate rows
Detect potential primary keys
Write cleaned data to S3 (TMP path)

**Key Tools**
get_s3_file_metadata
create_data_profile
execute_quality_checks
handle_duplicates_and_detect_keys

**Quality Checks Performed**
Not-null validation
Uniqueness checks
Duplicate row detection
Constant columns
Missing value thresholds (>30%)


**2. DDL Generator Agent**

**Purpose**
Automatically generates Redshift-compatible DDL.

**Responsibilities**
Infer schema from sample data
Map Pandas types → Redshift types
Normalize column names
Create staging & target tables
Apply primary key constraints
Write DDL to S3
Archive source files

**Key Tools**
get_sample_data
get_file_schema
write_sql_to_s3
archive_s3_file


**3. Redshift Setup & Loader Agent**

**Purpose**
Executes DDL and loads data into Redshift.

**Responsibilities**
Execute DDL
Copy data from S3 → staging
Perform SCD Type 2 upsert

**Key Tools**
execute_ddl_in_redshift
copy_data_to_staging
scd2_upsert_redshift


**Error Handling & Retries**
All Redshift operations retry up to 3 times
Exact Redshift error messages are returned on failure
Execution halts if critical steps fail


**Supported File Formats-:**
CSV
JSON (newline-delimited)
Parquet

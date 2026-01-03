import json
import boto3
import time
import io,os
import pandas as pd
from strands.tools import tool
from strands.agent import Agent
from strands.models import BedrockModel

# Explicitly pin AWS clients to ap-southeast-2
s3 = boto3.client("s3", region_name="ap-southeast-2")
rsd = boto3.client("redshift-data", region_name="ap-southeast-2")


def lambda_handler(event, context):
    for record in event["Records"]:
        s3_event = json.loads(record["body"])
        bucket_name = s3_event["Records"][0]["s3"]["bucket"]["name"]
        file_key = s3_event["Records"][0]["s3"]["object"]["key"]

        print("bucket_name:", bucket_name)
        print("file_key:", file_key)

        file_ext = os.path.splitext(file_key)[1].lower()
        print("file_ext:", file_ext)

        TARGET_TABLE = file_key[4:].split(".")[0]
        print("TARGET_TABLE:", TARGET_TABLE)

        sql_file_name = f"{TARGET_TABLE}.sql"
        print("sql_file_name:", sql_file_name)

        sql_key = f"{os.environ['DDL_PATH']}/{sql_file_name}"
        print("sql_key:", sql_key)

        raw_S3_URI = f"s3://{bucket_name}/{file_key}"
        print("raw_S3_URI:", raw_S3_URI)

        arc_S3_URI = (
            f"s3://{bucket_name}/{os.environ['ARC_PATH']}/{file_key[4:]}"
        )
        print("arc_S3_URI:", arc_S3_URI)

        # Environment variables
        REDSHIFT_DB = os.environ["REDSHIFT_DB"]
        TARGET_SCHEMA = os.environ["TARGET_SCHEMA"]
        STAGING_SCHEMA = os.environ["STAGING_SCHEMA"]
        REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
        IAM_ROLE_ARN = os.environ["IAM_ROLE_ARN"]

        print("REDSHIFT_DB", REDSHIFT_DB)
        print("TARGET_SCHEMA", TARGET_SCHEMA)
        print("TARGET_TABLE", TARGET_TABLE)
        print("REDSHIFT_WORKGROUP", REDSHIFT_WORKGROUP)
        print("IAM_ROLE_ARN", IAM_ROLE_ARN)

        def wait_for_redshift_execution(statement_id):
            while True:
                status = rsd.describe_statement(Id=statement_id)
                state = status["Status"]
                if state in ["FINISHED", "FAILED"]:
                    return status
                time.sleep(5)

        def target_exists(rsd, database, workgroup, target_schema, table_name) -> bool:
            sql = f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{target_schema}'
            AND table_name = '{table_name}';
            """
            resp = rsd.execute_statement(Database=database, Sql=sql, WorkgroupName=workgroup)
            status = wait_for_redshift_execution(resp["Id"])
            if status["Status"] != "FINISHED":
                return False
            result = rsd.get_statement_result(Id=resp["Id"])
            return int(result["Records"][0][0]["longValue"]) > 0



        # -------------------------------
        # Tools
        # -------------------------------

        @tool
        def get_s3_file_metadata(bucket: str, key: str) -> dict:
            try:
                response = s3.head_object(Bucket=bucket, Key=key)
                return {
                    "Size": response["ContentLength"],
                    "ContentType": response.get("ContentType", "unknown"),
                    "LastModified": str(response["LastModified"]),
                }
            except Exception as e:
                return {"error": str(e)}

        @tool
        def create_data_profile(bucket: str, key: str, file_type: str) -> dict:
            """
            Profiles the dataset (row count, column count, missing %, distinct counts, numeric stats).
            Relies on `file_type` determined by get_s3_file_metadata.
            """
            print(bucket,key,file_type)
            obj = s3.get_object(Bucket=bucket, Key=key)

            if file_type == "csv":
                df = pd.read_csv(obj["Body"])
            elif file_type == "json":
                df = pd.read_json(obj["Body"], lines=True)
            elif file_type == "parquet":
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            else:
                return {"error": f"Unsupported file type: {file_type}"}

            profile = {
                "rows": len(df),
                "columns": df.shape[1],
                "missing_pct": df.isnull().mean().round(3).to_dict(),
                "distinct_count": {col: df[col].nunique() for col in df.columns},
            }
            numeric_cols = df.select_dtypes(include=["int64", "float64", "int32", "float32"]).columns
            profile["numeric_summary"] = df[numeric_cols].describe().to_dict()
            print(profile,"profile")

            return profile

        @tool
        def execute_quality_checks(bucket: str, key: str, file_type: str, rules: dict) -> dict:
            """
            Executes data quality checks based on rules.
            `file_type` comes from metadata agent.
            """
            obj = s3.get_object(Bucket=bucket, Key=key)

            if file_type == "csv":
                df = pd.read_csv(obj["Body"])
            elif file_type == "json":
                df = pd.read_json(obj["Body"], lines=True)
            elif file_type == "parquet":
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            else:
                return {"error": f"Unsupported file type: {file_type}"}

            results = {}
            for col in rules.get("not_null", []):
                results[f"{col}_not_null"] = bool(df[col].notnull().all())
            for col in rules.get("unique", []):
                results[f"{col}_unique"] = bool(df[col].is_unique)
            for col in df.columns:
                if df[col].nunique() == 1:
                    results[f"{col}_constant"] = True  
            missing_ratio = df.isnull().mean()
            for col, ratio in missing_ratio.items():
                results[f"{col}_too_many_missing"] = ratio > 0.3
            results["duplicate_rows"] = bool(df.duplicated().any())
            print("result",results)

            return results

        @tool
        def handle_duplicates_and_detect_keys(bucket: str, key: str, output_key:str ,file_type: str, profile: dict, results: dict) -> dict:
            """
            - Removes duplicate rows if detected in `results`.
            - Identifies potential primary keys based on profile distinct counts.
            - Returns cleaned row count and candidate PKs.
            """

            # Load dataset
            obj = s3.get_object(Bucket=bucket, Key=key)
            if file_type == "csv":
                df = pd.read_csv(obj["Body"])
            elif file_type == "json":
                df = pd.read_json(obj["Body"], lines=True)
            elif file_type == "parquet":
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            else:
                return {"error": f"Unsupported file type: {file_type}"}

            # Step 1: Handle duplicates
            duplicates_removed = 0
            if results.get("duplicate_rows", False):
                before = len(df)
                df = df.drop_duplicates()
                after = len(df)
                duplicates_removed = before - after

            cleaned_csv=f"{TARGET_TABLE}.csv"
            s3.put_object(Bucket=bucket, Key=output_key, Body=df.to_csv(cleaned_csv,index=False))
            print( f"SQL written to s3://{bucket}/{output_key}")


            # Step 2: Detect potential primary keys
            potential_keys = []
            row_count = profile.get("rows", len(df))
            for col, distinct_val in profile.get("distinct_count", {}).items():
                if distinct_val == row_count:  # fully unique
                    potential_keys.append(col)

            # Step 3: Return metadata
            metadata = {
                "cleaned_rows": len(df),
                "duplicates_removed": duplicates_removed,
                "potential_keys": potential_keys,
            }

            return metadata

        @tool
        def get_sample_data(bucket: str, key: str, file_type:str,rows: int = 100) -> str: 
            obj = s3.get_object(Bucket=bucket, Key=key)
            if file_type == "csv":
                df = pd.read_csv(obj["Body"], nrows=rows)
                return df.to_csv(index=False)
            elif file_type == "json":
                df = pd.read_json(obj["Body"], lines=True)
                return df.head(objects).to_json(orient="records")          
            elif file_type == "parquet":
                df = pd.read_parquet(obj["Body"])
                return df.head(rows).to_csv(index=False)
            else:
                return {"error": f"Unsupported file type: {file_type}"}

        @tool 
        def get_file_schema(bucket: str, key: str, file_type:str) -> str: 
            print(file_type)
            obj = s3.get_object(Bucket=bucket, Key=key)
            if file_type == "csv":
                df = pd.read_csv(obj["Body"])
            elif file_type == "json":
                df = pd.read_json(obj["Body"], lines=True)
            elif file_type == "parquet":
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            else:
                return {"error": f"Unsupported file type: {file_type}"}

            schema = [f"{col}: {dtype}" for col, dtype in df.dtypes.items()] 
            return "\n".join(schema)

        @tool
        def write_sql_to_s3(bucket: str, key: str, sql: str) -> str:
            s3.put_object(Bucket=bucket, Key=output_key, Body=sql.encode("utf-8"))
            return f"SQL written to s3://{bucket}/{key}"

        @tool
        def archive_s3_file(bucket: str, source_key: str, archive_key: str) -> str:
            s3.copy_object(
                Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=archive_key
            )
            s3.delete_object(Bucket=bucket, Key=source_key)
            return f"Archived {source_key} to {archive_key}"

        @tool
        def execute_ddl_in_redshift(bucket: str, key: str, database: str, workgroup: str) -> str:
            print(bucket,key,database,workgroup,"all the details")
            #print("Using region before:", boto3.session.Session().region_name)
            rsd = boto3.client("redshift-data", region_name="ap-southeast-2")
            print("Using region:", boto3.session.Session().region_name)
            obj = s3.get_object(Bucket=bucket, Key=key)
            sql_content = obj["Body"].read().decode("utf-8")

            try:
                response = rsd.execute_statement(
                    Database=database,
                    Sql=sql_content,
                    WorkgroupName=workgroup   
                )
                status = wait_for_redshift_execution(response["Id"])
                return (
                    "Success"
                    if status["Status"] == "FINISHED"
                    else f"Failed: {status.get('Error', 'Unknown error')}"
                )
            except Exception as e:
                return f"Error executing DDL: {str(e)}"


        @tool
        def copy_data_to_staging(
            table_name: str, staging_schema: str, s3_uri: str,
            iam_role: str, database: str, workgroup: str
        ) -> str:
            rsd = boto3.client("redshift-data", region_name="ap-southeast-2")

            truncate_sql= f"""
            truncate table  {staging_schema}.{table_name};
            """
            try:
                response = rsd.execute_statement(
                    Database=database, Sql=truncate_sql, WorkgroupName=workgroup
                )
                status = wait_for_redshift_execution(response["Id"])
                if status["Status"] == "FINISHED":
                    copy_sql = f"""
                    COPY {staging_schema}.{table_name}
                    FROM '{s3_uri}'
                    IAM_ROLE '{iam_role}'
                    FORMAT AS CSV
                    IGNOREHEADER 1
                    BLANKSASNULL
                    EMPTYASNULL
                    TRIMBLANKS
                    ACCEPTINVCHARS;
                    """
                    try:
                        response = rsd.execute_statement(
                            Database=database, Sql=copy_sql, WorkgroupName=workgroup
                        )
                        status = wait_for_redshift_execution(response["Id"])
                        return "Success" if status["Status"] == "FINISHED" else f"Failed: {status.get('Error', 'Unknown error')}"
                    except Exception as e:
                        return f"Error while copy the data to stg: {str(e)}"

                else:
                    print(f"Failed: {status.get('Error', 'Unknown error')}")
            
            except Exception as e:
                return f"Error while truncating data: {str(e)}"
                
        @tool
        def scd2_upsert_redshift(
            table_name: str,
            staging_schema: str,
            target_schema: str,
            database: str,
            workgroup: str,
            iam_role:str,
            business_keys: list,
            valid_from_col: str = "valid_from",
            valid_to_col: str = "valid_to",
            is_current_col: str = "is_current",
            end_of_time: str = "9999-12-31 23:59:59",
            region_name: str = "ap-southeast-2"
        ) -> str:
            """
            Performs SCD Type 2 upsert from staging → target in Redshift.
            Steps:
            1. Discover target schema columns
            2. Ensure SCD2 columns exist
            3. Derive attributes
            4. Expire changed rows
            5. Insert new versions
            """
            if not business_keys or not isinstance(business_keys, list):
                return json.dumps({"error": "business_keys must be a non-empty list of column names."})

            rsd = boto3.client("redshift-data", region_name=region_name)

            # -------- helper --------
            def execute_and_wait(sql: str):
                try:
                    resp = rsd.execute_statement(
                        Database=database,
                        Sql=sql,
                        WorkgroupName=workgroup
                    )
                    status = wait_for_redshift_execution(resp["Id"])
                    return status
                except Exception as e:
                    return {"Status": "FAILED", "Error": str(e)}

            try:
                print(target_schema,table_name,"debugging 1 1 1 1 1 1")
                # 1) Discover target columns
                cols_sql = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{target_schema}'
                AND table_name   = '{table_name}'
                ORDER BY ordinal_position;
                """
                resp = rsd.execute_statement(Database=database, Sql=cols_sql, WorkgroupName=workgroup)
                status = wait_for_redshift_execution(resp["Id"])
                if status["Status"] != "FINISHED":
                    return json.dumps({"error": "Could not fetch columns", "details": status})

                result = rsd.get_statement_result(Id=resp["Id"])
                print("result",result)
                target_cols = [row[0]["stringValue"] for row in result["Records"]]
                print("target_cols",target_cols)

                # 2) Ensure SCD2 columns
                scd_set = {valid_from_col, valid_to_col, is_current_col}
                missing_scd = [c for c in scd_set if c not in target_cols]
                alter_results = []
                for col in missing_scd:
                    if col in (valid_from_col, valid_to_col):
                        alter = f"ALTER TABLE {target_schema}.{table_name} ADD COLUMN {col} TIMESTAMP;"
                    else:
                        alter = f"ALTER TABLE {target_schema}.{table_name} ADD COLUMN {col} BOOLEAN;"
                    alter_results.append(execute_and_wait(alter))

                # 3) Derive attribute columns
                attr_cols = [c for c in target_cols if c not in set(business_keys) and c not in scd_set]
                if not attr_cols:
                    return json.dumps({
                        "error": "No attribute columns found",
                        "details": {"target_columns": target_cols, "business_keys": business_keys}
                    })    
                print("attr_cols",attr_cols)     


                # 4) Expire changed rows
                expire_sql = f"""
                WITH ranked_stg AS (
                SELECT
                    stg.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY employee_id
                        ORDER BY eff_date DESC
                    ) AS rn
                FROM sch_stg.scd2_testing stg
                    ),  
                    latest_stg AS (
                        SELECT *
                        FROM ranked_stg
                        WHERE rn = 1
                    )
                    UPDATE sch_tgt.scd2_testing
                    SET valid_to = GETDATE(),
                        is_current = FALSE
                    WHERE is_current = TRUE
                    AND EXISTS (
                    SELECT 1
                    FROM latest_stg stg
                    WHERE sch_tgt.scd2_testing.employee_id = stg.employee_id
                    AND MD5(stg.name || '||' || stg.department || '||' || stg.salary)
                        <> MD5(sch_tgt.scd2_testing.name || '||' || sch_tgt.scd2_testing.department || '||' || sch_tgt.scd2_testing.salary)
            );
            """

                expire_status = execute_and_wait(expire_sql)
                print("expire_status",expire_status)

                # 5) Insert new versions
                insert_sql = f"""
                INSERT INTO sch_tgt.scd2_testing
                (employee_id, name, department, salary, eff_date, valid_from, valid_to, is_current)
                    WITH ranked_stg AS (
                        SELECT
                            stg.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY employee_id
                                ORDER BY eff_date DESC
                            ) AS rn
                        FROM sch_stg.scd2_testing stg
                    ),  
                    latest_stg AS (
                        SELECT *
                        FROM ranked_stg
                        WHERE rn = 1
                    )
                    SELECT 
                        stg.employee_id,
                        stg.name,
                        stg.department,
                        stg.salary,
                        stg.eff_date,
                        GETDATE(),
                        '9999-12-31 23:59:59',
                        TRUE
                    FROM latest_stg stg
                    LEFT JOIN sch_tgt.scd2_testing tgt
                    ON tgt.employee_id = stg.employee_id
                    AND tgt.is_current = TRUE
                    WHERE tgt.employee_id IS NULL
                    OR MD5(stg.name || '||' || stg.department || '||' || stg.salary)
                <> MD5(tgt.name || '||' || tgt.department || '||' || tgt.salary);
                """


                insert_status = execute_and_wait(insert_sql)
                print("insert_status",insert_status)


                return json.dumps({
                    "status": "completed",
                    # "alter_results": alter_results,
                    # "expire_status": expire_status,
                    # "insert_status": insert_status,
                    "attr_cols": attr_cols,
                    "business_keys": business_keys
                })

            except Exception as e:
                return json.dumps({"error": str(e)})


        # -------------------------------
        # Agent model (Bedrock stays in us-west-2)
        # -------------------------------
        model = BedrockModel(
            model_id="anthropic.claude-3-sonnet-20240229-v1:0", 
            region="us-west-2",
        )


        #------------------
        #AGENT 1
        #------------------
        qa_agent = Agent(
        name="Quality Assurance Agent",
        description="Ensures data quality before loading into Redshift by profiling, validating rules, and detecting anomalies.",
        tools=[
            get_s3_file_metadata,   # always run first to detect file info
            create_data_profile,    # profiling (missing %, distincts, stats)
            execute_quality_checks, # validation rules (not null, unique, ranges, allowed values)
            handle_duplicates_and_detect_keys],
        model=model,
        )

        #------------------
        #AGENT 2
        #------------------
        ddl_generator = Agent(
            name="DDL Generator Agent",
            description="Infers schema and generates Redshift DDL from S3 data.",
            tools=[get_sample_data,get_file_schema, write_sql_to_s3, archive_s3_file],
            model=model,
        )

        #------------------
        #AGENT 3
        #------------------

        redshift_agent_full= Agent(
            name="Redshift Setup & Loader",
            description="Creates Redshift table and loads data from S3.",
            tools=[execute_ddl_in_redshift, copy_data_to_staging,scd2_upsert_redshift],
            model=model,
        )


        redshift_agent_incremental = Agent(
            name="Redshift Loader Only",
            description="Loads data from S3 and applies SCD2 (no DDL).",
            tools=[copy_data_to_staging, scd2_upsert_redshift],
            model=model, )


        #----------------
        #calling agents
        #----------------
        rsd = boto3.client("redshift-data", region_name="ap-southeast-2")
        if not target_exists(rsd, REDSHIFT_DB, REDSHIFT_WORKGROUP, TARGET_SCHEMA, TARGET_TABLE):
            
            #------------------CALLING AGENT 1---------------------------------

            qa_response = qa_agent(f"""
            You are the Quality Assurance Agent. Your task is to validate data quality for a file in S3 
            before it is loaded into Redshift. Always start by running `get_s3_file_metadata` 
            to detect the file type and metadata. Then, depending on the metadata and file type, 
            perform the following:

            File Details:
            - Bucket: '{bucket_name}'
            - Key: '{file_key}'
            - output_key: '{os.environ['TMP_PATH']}'
            - File name : 'Fetched from metadata'

            1. Use `create_data_profile` to summarize the dataset (row/column counts, missing %, distincts, numeric stats).
            2. Apply `execute_quality_checks` with rules such as:
            - `not_null`: on only columns having missing_pct = 0
            - `unique`: on all the columns having disctinct_count 0

            3. Run `handle_duplicates_and_detect_keys` to remove duplicates & write the file to the tmp bucket & identify potential primary key using ouput from previous execution
            considering the input 'profile' and 'results' as JSON not as python dict.
            Finally, return a structured quality report that includes:
            - File metadata (size, content type, last modified).
            - Profiling summary.
            - Rule validation results.
            - remove duplicates and primary keys.

            If any rules fail, clearly highlight them in the report.
            """)


            #------------------CALLING AGENT 2---------------------------------
            ddl_response = ddl_generator(
            f"""
            You are tasked with generating a Redshift-compatible DDL using the provided cleaned schema.
            
            File Details:
            - Bucket: '{bucket_name}'
            - Key: '{file_key}'
            - file type: we got from 'get_s3_file_metadata'
            - Potential Primary Keys: Use `potential_keys` from metadata output 
            - Target Table: '{TARGET_TABLE}'
            - Target Schema: '{TARGET_SCHEMA}'
            - Staging Schema: '{STAGING_SCHEMA}'
            - DDL Output Key: '{sql_key}'
            - Archive Path: '{os.environ['ARC_PATH']}'

            Instructions:
            1. Get the sample for the file using 'get_sample_data' tool to infer the type of data in each column. 
            2. Infer the schema using 'get_file_schema', but apply the following rules to map to Redshift types:
            - If a column contains values in the format YYYY-MM-DD or DD-MM-YYYY, map it to DATE.  
            - If it contains datetime/timestamp values, map it to TIMESTAMP.  
            - Only use VARCHAR(MAX) when column values are clearly non-numeric, non-date strings.  
            - For numeric values, use INT8, DECIMAL, or FLOAT depending on precision.  
            3. Replace unsupported types (like 'object') with 'VARCHAR(MAX)'.
            4. Create both schemas '{STAGING_SCHEMA}' and '{TARGET_SCHEMA}' if they do not exist.
            5. Use the `potential_keys` from metadata (output of handle_duplicates_and_detect_keys) 
               Add PRIMARY KEY constraint(s) using the columns provided in `potential_keys`.  
            - If multiple values(columns) are present in potential_keys, create a composite primary key.  
            - If no keys are detected, skip PK definition.  
            to define PRIMARY KEY constraints in the target table DDL.
            6. Create a staging table '{STAGING_SCHEMA}.{TARGET_TABLE}' and a target table '{TARGET_SCHEMA}.{TARGET_TABLE}' 
            with the same schema if it does not exist.
            7. Ensure PRIMARY KEY constraint(s) using the columns provided in `potential_keys`.  
            - If multiple values(columns) are present in potential_keys, create a composite primary key.  
            - If no keys are detected, skip PK definition.  
            8. Normalize column names for Redshift (snake_case, no spaces, no special characters).
            9. Ensure CREATE statements use IF NOT EXISTS to avoid conflicts.
            10. Save the generated DDL to S3 at '{sql_key}'.
            11. After successful DDL generation, archive the original file to '{os.environ['ARC_PATH']}'.

            Error Handling:
            - If any step fails, retry up to 3 times.
            - If it still fails after retries, stop execution and return the exact Redshift error message.
            """
        )


            print("DDL Generator Response:", ddl_response)

        #----------------------CALLING AGENT 3---------------------------------

            ddl_execution_status = redshift_agent_full(f"""
                You must execute the DDL using the tool `execute_ddl_in_redshift`.

                Use the following exact parameters:
                - bucket: "{bucket_name}"
                - key: "{sql_key}"
                - database: "{REDSHIFT_DB}"
                - Target Schema: "{TARGET_SCHEMA}"
                - Staging Schema: "{STAGING_SCHEMA}"
                - workgroup: "{REDSHIFT_WORKGROUP}"

                Do not invent or substitute values.
                Only use these values exactly as provided.
                If execution fails,retry for maximum 3 tries and then return the exact Redshift error message if max tries reached.
                """)

            print("Redshift Agent Response:", ddl_execution_status)

            if "successful" in str(ddl_execution_status).lower():
                print("DDL executed successfully.")
                full_load_status = redshift_agent_full(
                    f"""
                    Load whole data from '{arc_S3_URI}' into Redshift table '{STAGING_SCHEMA}.{TARGET_TABLE}'.
                    Use the following exact parameters:
                    - database: "{REDSHIFT_DB}"
                    - iam role: "{IAM_ROLE_ARN}"
                    - Schema: '{STAGING_SCHEMA}'
                    - workgroup: "{REDSHIFT_WORKGROUP}"
                    Use copy_data_to_staging.
                    If while copying any of the table is loaded incorrectly please load the data again for that table, try for max 3 times, if not then
                    return the exact Redshift error message.
                    """) 


                print("Redshift Agent load Response:", full_load_status)
                if "successful" in str(full_load_status).lower():
                    scd2_status = redshift_agent_full(
                    f"""
                    Perform an SCD Type 2 upsert from staging table '{STAGING_SCHEMA}.{TARGET_TABLE}'
                    into target table '{TARGET_SCHEMA}.{TARGET_TABLE}'.

                    Use the following exact parameters:
                    - target schema: "{TARGET_SCHEMA}"
                    - table name: "{TARGET_TABLE}"
                    - database: "{REDSHIFT_DB}"
                    - workgroup: "{REDSHIFT_WORKGROUP}"
                    - IAM role: "{IAM_ROLE_ARN}"
                    - business keys: ['employee_id']
                    - valid_from_col: "valid_from"
                    - valid_to_col: "valid_to"
                    - is_current_col: "is_current"
                    - end_of_time: "9999-12-31 23:59:59"
                    - region: "ap-southeast-2"

                    Steps to follow:
                    1. Discover all columns in the target table.
                    2. Ensure that SCD2 columns (valid_from, valid_to, is_current) exist. 
                    If missing, add them.
                    3. Identify attribute columns (target minus keys minus SCD columns).
                    4. Expire existing rows (set valid_to = now, is_current = false) only 
                    when any attribute value has changed.
                    5. Insert new rows with updated attributes, setting valid_from = now, 
                    valid_to = end_of_time, and is_current = true.
                    6. Return JSON with:
                    - alter results
                    - expire status
                    - insert status
                    - business keys and attribute columns used

                    Use scd2_upsert_redshift tool.
                    If the operation fails, return the exact Redshift error message.
                    """)
        else:

            acrchving_file = ddl_generator(
            f"""
            You are task is to archive the file using tool 'archive_s3_file'
            File Details:
            - Bucket: '{bucket_name}'
            - Source Key: '{file_key}'
            - Archive Key: '{os.environ['ARC_PATH']}/{file_key[4:]}'

            Instructions:
            Archive file from 'raw_S3_URI' file to 'arc_S3_URI'.
            """)

            incremental_load_status = redshift_agent_incremental(
            f"""Truncate the table '{STAGING_SCHEMA}.{TARGET_TABLE}' and then 
            Load whole data from '{arc_S3_URI}' into Redshift table '{STAGING_SCHEMA}.{TARGET_TABLE}'.
            Use the following exact parameters:
            - database: "{REDSHIFT_DB}"
            - iam role: "{IAM_ROLE_ARN}"
            - Schema: '{STAGING_SCHEMA}'
            - workgroup: "{REDSHIFT_WORKGROUP}"
            Use copy_data_to_staging.
            If while copying any of the table is loaded incorrectly please load the data again for that table, try for max 3 times, if not then
            return the exact Redshift error message.
            """) 

            print("Redshift Agent load Response:", incremental_load_status)
            if "successfully" in str(incremental_load_status).lower():
                scd2_status = redshift_agent_incremental(
                    f"""
                    Perform an SCD Type 2 upsert from staging table '{STAGING_SCHEMA}.{TARGET_TABLE}'
                    into target table '{TARGET_SCHEMA}.{TARGET_TABLE}'.

                    Use the following exact parameters:
                    - target schema: "{TARGET_SCHEMA}"
                    - table name: "{TARGET_TABLE}"
                    - database: "{REDSHIFT_DB}"
                    - workgroup: "{REDSHIFT_WORKGROUP}"
                    - IAM role: "{IAM_ROLE_ARN}"
                    - business keys: ['employee_id']
                    - valid_from_col: "valid_from"
                    - valid_to_col: "valid_to"
                    - is_current_col: "is_current"
                    - end_of_time: "9999-12-31 23:59:59"
                    - region: "ap-southeast-2"

                    Steps to follow:
                    1. Discover all columns in the target table.
                    2. Ensure that SCD2 columns (valid_from, valid_to, is_current) exist. 
                    If missing, add them.
                    3. Identify attribute columns (target minus keys minus SCD columns).
                    4. Expire existing rows (set valid_to = now, is_current = false) only 
                    when any attribute value has changed.
                    5. Insert new rows with updated attributes, setting valid_from = now, 
                    valid_to = end_of_time, and is_current = true.
                    6. Return JSON with:
                    - alter results
                    - expire status
                    - insert status
                    - business keys and attribute columns used

                    Use scd2_upsert_redshift tool.
                    If the operation fails, return the exact Redshift error message.
                    """)


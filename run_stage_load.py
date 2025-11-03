#---------------------------------------------------------------------
# Name: run_stage_load.ipynb
#---------------------------------------------------------------------
# Purpose:  Stages registered files from the landing zone to 
#           delta stage tables in the bronze layer using expected
#           schema and source attributes from the meta data.
#---------------------------------------------------------------------
# ver.  | date     | author         | change
#---------------------------------------------------------------------
# v1    | 10/28/25 | K. Hardis      | Initial Version.
#---------------------------------------------------------------------

# Standard library
import sys
import fnmatch
import os
import re

# PySpark SQL functions
from pyspark.sql.functions import col, current_timestamp, lit

# PySpark types
from datetime import datetime

sys.path.append("./builtin")

# External Modules
import shared_context as sc
import mlUtil as ml

import importlib

# Force reload in case modules were cached
importlib.reload(sc)

# Log external module versions
from log_module_versions import log_module_versions
log_module_versions(["shared_context","mlUtil"])

print('||-------------run_stage_load.ipynb--------------||')
print('||-----')

# Create spark shared context
ctx = sc.SparkContextWrapper(spark)

# Load all REGISTERED files
registered_files_df = ctx.spark.sql("""
    SELECT * FROM lk_cdsa_bronze.meta_db.data_file
    WHERE file_status = 'REGISTERED'
""")

staged_count = 0
for file_row in registered_files_df.collect():
    try:
        file_id = file_row.file_id
        filename = file_row.filename
        object_id = file_row.object_id
        source_id = file_row.object_id
        batch_id = file_row.batch_id
        landing_directory = file_row.file_path

        full_path = f"abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse{landing_directory}/{filename}"
        print(f"\n|| Processing file_id: {file_id}")
        print(f"|| Filename: {filename}")
        print(f"|| Full path: {full_path}")

        # Load source attributes
        source_attr_df = ctx.spark.sql(f"""
            SELECT *, sf.source_id as source_feed_id, t.target_id as target_object_id
            FROM lk_cdsa_bronze.meta_db.source s
            JOIN lk_cdsa_bronze.meta_db.data_object do ON s.source_id = do.object_id
            LEFT JOIN lk_cdsa_bronze.meta_db.source_feed sf ON s.source_id = sf.source_id
            LEFT JOIN lk_cdsa_bronze.meta_db.target t ON sf.target_object_name = t.target_name
            WHERE s.source_id = '{source_id}'
        """)

        if source_attr_df.count() == 0:
            print(f"|| Skipping file_id {file_id}: No source attributes found.")
            continue

        source_row = source_attr_df.first()
        column_delimiter = source_row.column_delimiter or ","
        stage_name = source_row.stage_name

        # Generate stage table name
        stage_table_name_df = ctx.spark.sql(f"""
            SELECT UPPER(LOWER(c.stage_name) || '_' || RIGHT('0000000' || CAST(a.file_id AS STRING), 7)) AS table_name
            FROM lk_cdsa_bronze.meta_db.data_file a
            JOIN lk_cdsa_bronze.meta_db.source c ON a.object_id = c.source_id
            WHERE a.file_id = {file_id}
        """)
        stage_table_name = stage_table_name_df.first()["table_name"]
        print(f"|| Stage table name: {stage_table_name}")

        # Get source feed columns
        feed_columns_df = ctx.spark.sql(f"""
            SELECT column_name, data_type, max_length, scale
            FROM lk_cdsa_bronze.meta_db.source_feed_column
            WHERE source_id = '{source_id}'
            ORDER BY ordinal_position, column_id
        """)
        feed_columns = feed_columns_df.collect()

        # Load file
        df = ctx.spark.read \
            .option("header", True) \
            .option("delimiter", column_delimiter) \
            .csv(full_path)

        print(f"|| Records read from file: {df.count()}")

        # Normalize column names
        df = df.toDF(*[col_name.lower() for col_name in df.columns])

        # Check for schema mismatch
        expected_columns = [row.column_name.lower() for row in feed_columns]
        df_columns = [col.lower() for col in df.columns]

        if set(expected_columns) != set(df_columns):
            print(f"|| Schema mismatch for file_id {file_id}.")
            print(f"|| Expected columns: {expected_columns}")
            print(f"|| Found columns: {df_columns}")
            continue

        # Cast columns to expected types
        for row in feed_columns:
            col_name = row.column_name.lower()
            expected_type = row.data_type.lower()

            if expected_type == "decimal":
                df = df.withColumn(col_name, col(col_name).cast(f"decimal({int(row.max_length)},{int(row.scale)})"))
            elif expected_type in ["char", "varchar", "string"]:
                df = df.withColumn(col_name, col(col_name).cast("string"))
            elif expected_type in ["bigint", "long"]:
                df = df.withColumn(col_name, col(col_name).cast("long"))
            elif expected_type in ["int", "integer"]:
                df = df.withColumn(col_name, col(col_name).cast("int"))
            elif expected_type == "double":
                df = df.withColumn(col_name, col(col_name).cast("double"))
            elif expected_type == "float":
                df = df.withColumn(col_name, col(col_name).cast("float"))
            elif expected_type == "boolean":
                df = df.withColumn(col_name, col(col_name).cast("boolean"))
            elif expected_type == "date":
                df = df.withColumn(col_name, col(col_name).cast("date"))
            elif expected_type == "timestamp":
                df = df.withColumn(col_name, col(col_name).cast("timestamp"))
            else:
                df = df.withColumn(col_name, col(col_name).cast("string"))  # fallback

        # Reorder columns
        df = df.select(*expected_columns)

        # Create stage table
        column_defs = []
        for row in feed_columns:
            col_name = row.column_name.lower()
            data_type = row.data_type.lower()

            if data_type in ["char", "varchar"]:
                col_def = f"{col_name} string"
            elif data_type == "decimal":
                col_def = f"{col_name} decimal({int(row.max_length)},{int(row.scale)})"
            else:
                col_def = f"{col_name} {data_type}"

            column_defs.append(col_def)

        # Add real_customer_score as float
        column_defs.append("real_customer_score float")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS lk_cdsa_bronze.bronze_db.{stage_table_name} (
            {', '.join(column_defs)}
        ) USING DELTA
        """
        ctx.spark.sql(create_table_sql)

        mapping_rules = {
            "first_name": ["first", "firstname", "first_name"],
            "last_name": ["last", "lastname", "last_name"],
            "email": ["email"],
            "date_of_birth": ["dob", "date_of_birth"],
            "address": ["street_address", "address"],
            "zip_code": ["postal", "zip", "zip_code"],
            "company_name": ["company", "company_name"],
            "phone": ["phone", "phone_number"]
        }

        column_map = {}
        for model_col, patterns in mapping_rules.items():
            for col in df.columns:
                if any(re.fullmatch(pattern, col, re.IGNORECASE) for pattern in patterns):
                    column_map[col] = model_col
                    break

        print(f"|| Scoring staged customers in {stage_table_name}")
        model_path = "abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse/Files/ml_models/synthetic_customer_logistic_model"
        scored_df = ml.score_customers(df, model_path, column_map)
        print(f"|| real_customer_score appended to {stage_table_name}")

        reverse_column_map = {v: k for k, v in column_map.items() if v}
        for std_col, orig_col in reverse_column_map.items():
            if std_col in scored_df.columns:
                scored_df = scored_df.withColumnRenamed(std_col, orig_col)

        # Get original column names
        original_columns = df.columns

        # Select only original columns + real_customer_score from scored_df
        df = scored_df.select(*original_columns, "real_customer_score")

        # Write to stage table
        output_table = f"lk_cdsa_bronze.bronze_db.{stage_table_name}"
        df.write.format("delta").mode("append").saveAsTable(output_table)

        row_count = df.count()
        print(f"|| Records staged to {stage_table_name}: {row_count}")

        # Update metadata
        ctx.spark.sql(f"""
            UPDATE lk_cdsa_bronze.meta_db.data_file
            SET row_count = {row_count}, file_status = 'COMPLETED'
            WHERE file_id = {file_id}
        """)

        print(f"|| Staged file_id {file_id} successfully.")
        staged_count += 1

    except Exception as e:
        print(f"|| Error processing file_id {file_row.file_id}: {str(e)}")


# Final output
if staged_count == 0:
    print("|| No REGISTERED files were staged.")
    print("||----------------SKIPPED----------------||")
else:
    print(f"|| Stage load completed successfully for {staged_count} file(s).")
    print("||----------------SUCCESS----------------||")

print('||-----')

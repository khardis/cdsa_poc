#---------------------------------------------------------------------
# Name: clnt_receive_data_file.ipynb
#---------------------------------------------------------------------
# Purpose:  Automates scanning of landing zone directories in OneLake/ADLS, 
#           matching incoming files to expected patterns (from meta-data), 
#           and registering new file discoveries in the data_file meta table.
#---------------------------------------------------------------------
# ver.  | date     | author         | change
#---------------------------------------------------------------------
# v1    | 10/28/25 | K. Hardis      | Initial Version.
#---------------------------------------------------------------------

# Standard library
import sys
import fnmatch
import os

# PySpark SQL functions
from pyspark.sql.functions import current_timestamp

# PySpark types
from datetime import datetime

sys.path.append("./builtin")

# External Modules
import shared_context as sc
import opsLookupUtil as lkp

import importlib

# Force reload in case modules were cached
importlib.reload(sc)
importlib.reload(lkp)

# Log external module versions
from log_module_versions import log_module_versions
log_module_versions(["shared_context","opsLookupUtil"])


print('||-------------clnt_receive_data_file.ipynb--------------||')
print('||-----')


# Create spark shared context
ctx = sc.SparkContextWrapper(spark)

batch_name = 'daily_update'
batchAttr = lkp.getActiveBatchRecordByBatchName(ctx, batch_name)

if batchAttr is None or batchAttr.rdd.isEmpty():
    errorMSG = 'No Records Found in BATCH! Exiting...'
    print('|| ' + errorMSG)
    # upd.updateProcessLog(MetaEngine, process_log_id, None, None, None, None, None, None, None, None, None, None, None, failedStr, errorMSG, None)
    raise ValueError(errorMSG)
else:
    row = batchAttr.first()
    print(f"||    batch_id: {row['batch_id']}")
    print(f"||    batch_name: {row['batch_name']}")
    print('||-----')

# Get all file-type data objects
data_objects_df = ctx.spark.sql("""
    SELECT object_id, object_name, filename_pattern, landing_directory
    FROM meta_db.data_object
    WHERE object_type = 'file'
""").collect()

for obj in data_objects_df:
    data_source = obj["object_name"]
    object_id = obj["object_id"]
    pattern = obj["filename_pattern"].replace('%', '*')
    landing_directory = obj["landing_directory"]

    full_path = f"abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse{landing_directory}"

    print(f"||    Scanning path: {full_path}")
    print(f"||    Filename pattern: {pattern}")
    print(f"||    Processing files for data source: {data_source}")
    print('||-----')

    # Read file metadata
    file_df = ctx.spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(full_path)

    # Extract and filter filenames
    all_files = [row["path"].split("/")[-1] for row in file_df.select("path").collect()]
    filtered_files = [f for f in all_files if fnmatch.fnmatch(f, pattern)]

    # Extract and filter file paths
    all_paths = [row["path"] for row in file_df.select("path").collect()]
    filtered_paths = [p for p in all_paths if fnmatch.fnmatch(p.split("/")[-1], pattern)]

    # Extract relative directories
    relative_dirs = ["/Files" + os.path.dirname(p.split("/Files", 1)[-1]) for p in filtered_paths]

    # Compare against metadata table
    try:
        registered_df = ctx.spark.table("meta_db.data_file").select("filename")
        registered_files = [row.filename for row in registered_df.collect()]
    except:
        registered_files = []

    unregistered_files = [f for f in filtered_files if f not in registered_files]
    
    # Get unregistered file paths (directory only)
    unregistered_file_paths = [
        relative_dirs[i] for i, p in enumerate(filtered_paths)
        if p.split("/")[-1] not in registered_files
    ]

    # Get current max batch_id for daily_update in STARTED state
    batch_row = ctx.spark.sql("""
        SELECT COALESCE(MAX(batch_id), 0) AS max_id
        FROM meta_db.BATCH
        WHERE batch_name = 'daily_update' AND batch_status = 'STARTED'
    """).first()
    current_batch_id = batch_row["max_id"]

    # Get current max file_id
    try:
        file_id_row = ctx.spark.sql("SELECT COALESCE(MAX(file_id), 0) AS max_id FROM meta_db.data_file").first()
        next_file_id = file_id_row["max_id"] + 1
    except:
        next_file_id = 1

        file_id = file_row.file_id
        filename = file_row.filename
        object_id = file_row.object_id
        source_id = file_row.object_id
        batch_id = file_row.batch_id
        landing_directory = file_row.file_path

    if unregistered_files:
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for i, filename in enumerate(unregistered_files):
            file_path = unregistered_file_paths[i]  # Match index to filename
            ctx.spark.sql(f"""
                INSERT INTO meta_db.data_file
                VALUES (
                    {next_file_id + i},    -- file_id
                    {object_id},           -- object_id
                    NULL,                  -- file_pattern
                    NULL,                  -- file_date
                    TIMESTAMP('{now_ts}'), -- file_received_date
                    'REGISTERED',          -- file_status
                    NULL,                  -- file_byte_size
                    '{filename}',          -- filename
                    NULL,                  -- expected_row_count
                    NULL,                  -- row_count
                    NULL,                  -- fm_file_id
                    NULL,                  -- fm_good_record_count
                    NULL,                  -- fm_error_record_count
                    NULL,                  -- stg_good_record_count
                    '{file_path}',         -- file_path
                    {current_batch_id},    -- batch_id
                    TIMESTAMP('{now_ts}'), -- created_date
                    'system',              -- created_by
                    TIMESTAMP('{now_ts}'), -- modified_date
                    'system'               -- modified_by
                )
            """)
        print(f"||    Registered {len(unregistered_files)} new files:")
        for i, filename in enumerate(unregistered_files):
            file_id = next_file_id + i
            print(f"||        [file_id: {file_id}] {filename}")
    else:
        print("||    No new files to register.")

    print('||-----')

print('||----------------SUCCESS----------------||')
print('||-----')
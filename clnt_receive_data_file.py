# COMMAND ----------

# Notebook widgets for parameterization
dbutils.widgets.text("filename", "")
dbutils.widgets.text("record_count", "")
dbutils.widgets.text("expected_row_count", "")
dbutils.widgets.text("batch_name", "")
dbutils.widgets.text("status", "")

filename = dbutils.widgets.get("filename")
record_count = dbutils.widgets.get("record_count")
expected_row_count = dbutils.widgets.get("expected_row_count")
batch_name = dbutils.widgets.get("batch_name") or "daily_update"
status = dbutils.widgets.get("status") or "RECEIVED"

# COMMAND ----------

from pyspark.sql.functions import col, lit
from datetime import datetime

# Define paths to bronze delta tables
data_object_path = "Tables/lk_msft_bronze.data_object"
batch_path = "Tables/lk_msft_bronze.batch"
data_file_path = "Tables/lk_msft_bronze.data_file"

# COMMAND ----------

# Lookup metadata for the file from data_object table
data_object_df = spark.read.format("delta").load(data_object_path)
data_object_info = data_object_df.filter(col("filename") == filename).limit(1).collect()

if not data_object_info:
    raise ValueError(f"No metadata found for filename: {filename}")

data_object_info = data_object_info[0]
object_id = data_object_info["object_id"]
object_name = data_object_info["object_name"]
filename_pattern = data_object_info["filename_pattern"]
landing_directory = data_object_info["landing_directory"]
compression_type = data_object_info["compression_type"]

# COMMAND ----------

# Lookup batch_id from batch table
batch_df = spark.read.format("delta").load(batch_path)
batch_info = batch_df.filter(col("batch_name") == batch_name).select("batch_id").collect()

if not batch_info:
    raise ValueError(f"No active batch found for batch_name: {batch_name}")

batch_id = batch_info[0]["batch_id"]

# COMMAND ----------

# If record_count is not provided, try to infer it from the landed file
if not record_count:
    try:
        file_path = f"{landing_directory}/{filename}"
        if compression_type.lower() == "gzip" and not filename.endswith(".gz"):
            file_path += ".gz"

        raw_df = spark.read.text(file_path)
        record_count = raw_df.count()
    except Exception as e:
        print(f"WARNING: Could not determine record_count: {e}")
        record_count = None

# COMMAND ----------

# Create a new record for the data_file table
new_data_file_df = spark.createDataFrame([{
    "batch_id": batch_id,
    "filename": filename,
    "object_id": object_id,
    "filename_pattern": filename_pattern,
    "status": status,
    "expected_row_count": expected_row_count,
    "record_count": record_count,
    "landing_directory": landing_directory,
    "created_at": datetime.now()
}])

new_data_file_df.write.format("delta").mode("append").save(data_file_path)

# COMMAND ----------


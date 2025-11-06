import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, lit, when, udf, format_number, round
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType, StringType

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse/Files/ml_training_data/customer_data/synthetic_customer_test_data.csv")

# Load valid first names and ZIP codes
with open("/path/to/valid_first_names.json", "r") as f:
    VALID_FIRST_NAMES = json.load(f)

with open("/path/to/valid_zip_codes.json", "r") as f:
    VALID_ZIP_CODES = json.load(f)


# Define synthetic and valid patterns
synthetic_patterns = {
    "email": ["test", "admin", "null", "spamtrap", "example", "private", "none", "xyz"],
    "phone": ["9876543210", "911", "411", "5550101234", "0", "0000000000"],
    "name": ["test", "demo", "sample", "null", "placeholder", "mickey", "mouse"],
    "dob": ["1900-01-01", "1/1/1900"],
    "address": ["123 test st", "unknown", "n/a", "null", "fake", "placeholder"],
    "zip": ["00000", "12345", "99999"],
    "company": ["test", "demo", "sample", "unknown", "n/a", "null"]
}

valid_patterns = {
    "first_name": VALID_FIRST_NAMES,  # to be filled from generated list
    "zip_code": VALID_ZIP_CODES       # to be filled from generated list
}

# Create synthetic flags
df = df.withColumn("email_flag", when(lower(col("email")).rlike("(" + "|".join(synthetic_patterns["email"]) + ")"), 1).otherwise(0))
df = df.withColumn("phone_flag", when(col("phone").isin(synthetic_patterns["phone"]), 1).otherwise(0))
df = df.withColumn("name_flag", when(lower(col("first_name")).rlike("(" + "|".join(synthetic_patterns["name"]) + ")") |
                                      lower(col("last_name")).rlike("(" + "|".join(synthetic_patterns["name"]) + ")"), 1).otherwise(0))
df = df.withColumn("dob_flag", when(col("date_of_birth").isin(synthetic_patterns["dob"]), 1).otherwise(0))
df = df.withColumn("address_flag", when(lower(col("address")).rlike("(" + "|".join(synthetic_patterns["address"]) + ")"), 1).otherwise(0))
df = df.withColumn("zip_flag", when(col("zip_code").isin(synthetic_patterns["zip"]), 1).otherwise(0))
df = df.withColumn("company_flag", when(lower(col("company_name")).rlike("(" + "|".join(synthetic_patterns["company"]) + ")"), 1).otherwise(0))

# Create validity flags
df = df.withColumn("valid_name_flag", when(col("first_name").isin(valid_patterns["first_name"]), 1).otherwise(0))
df = df.withColumn("valid_zip_flag", when(col("zip_code").isin(valid_patterns["zip_code"]), 1).otherwise(0))

# Combine into hybrid score
df = df.withColumn("hybrid_score", col("valid_name_flag") + col("valid_zip_flag") -
                   (col("email_flag") + col("phone_flag") + col("name_flag") + col("dob_flag") +
                    col("address_flag") + col("zip_flag") + col("company_flag")))

# Label mapping
df = df.withColumn("indexed_label", when(col("label") == 0, 1).otherwise(0))  # 1 = real, 0 = synthetic

# Assemble features
assembler = VectorAssembler(
    inputCols=["email_flag", "phone_flag", "name_flag", "dob_flag", "address_flag", "zip_flag", "company_flag",
               "valid_name_flag", "valid_zip_flag", "hybrid_score"],
    outputCol="features"
)

# Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="indexed_label", predictionCol="prediction",
                        probabilityCol="probability", threshold=0.5)

# Pipeline
pipeline = Pipeline(stages=[assembler, lr])
train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)
model = pipeline.fit(train_df)

# Apply model
predictions = model.transform(df)
predictions = predictions.withColumn("is_real_customer", when(col("prediction") == 1, lit("Y")).otherwise(lit("N")))

# Confidence and score
extract_confidence = udf(lambda v: float(v[1]), DoubleType())
predictions = predictions.withColumn("confidence", extract_confidence(col("probability")))
predictions = predictions.withColumn("real_customer_score", round(col("confidence") * 10.0, 1).cast("float"))

# Display results
display(predictions.select("first_name", "last_name", "email", "phone", "date_of_birth", "address", "zip_code",
                           "company_name", "is_real_customer", "confidence", "real_customer_score"))


# PySpark MLlib Notebook: Detecting Test or Synthetic Customers

# This notebook trains a logistic regression model using PySpark MLlib to detect synthetic or test customers
# based on known placeholder patterns in fields like email, phone, name, DOB, address, zip code, and company name.
# It evaluates the model using accuracy, precision, recall, F1 score, and AUC, and visualizes the confusion matrix and ROC curve.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, lit, struct, when, udf, format_number
from pyspark.ml.feature import VectorAssembler, StringIndexerModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, roc_curve, auc
from pyspark.sql.types import DoubleType, StringType
import matplotlib.pyplot as plt
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load enriched labeled dataset
# This dataset should include columns like first_name, last_name, email, phone, date_of_birth, address, zip_code, company_name, and label
#df = spark.read.option("header", True).option("inferSchema", True).csv("abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse/Files/ml_training_data/customer_data/enriched_labeled_customer_data.csv")
df = spark.read.option("header", True).option("inferSchema", True).csv("abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse/Files/ml_training_data/customer_data/synthetic_customer_test_data.csv")

# Define synthetic patterns
email_indicators = ["test", "admin", "null", "spamtrap", "example", "private", "none", "xyz"]
phone_indicators = ["9876543210", "911", "411", "5550101234", "0", "0000000000"]
name_indicators = ["test", "demo", "sample", "null", "placeholder", "mickey", "mouse"]
dob_indicators = ["1900-01-01", "1/1/1900"]
address_indicators = ["123 test st", "unknown", "n/a", "null", "fake", "placeholder"]
zip_indicators = ["00000", "12345", "99999"]
company_indicators = ["test", "demo", "sample", "unknown", "n/a", "null"]

# Create indicator flags
df = df.withColumn("email_flag", when(lower(col("email")).rlike("(" + "|".join(email_indicators) + ")"), 1).otherwise(0))
df = df.withColumn("phone_flag", when(col("phone").isin(phone_indicators), 1).otherwise(0))
df = df.withColumn("name_flag", when(lower(col("first_name")).rlike("(" + "|".join(name_indicators) + ")") |
                                      lower(col("last_name")).rlike("(" + "|".join(name_indicators) + ")"), 1).otherwise(0))
df = df.withColumn("dob_flag", when(col("date_of_birth").isin(dob_indicators), 1).otherwise(0))
df = df.withColumn("address_flag", when(lower(col("address")).rlike("(" + "|".join(address_indicators) + ")"), 1).otherwise(0))
df = df.withColumn("zip_flag", when(col("zip_code").isin(zip_indicators), 1).otherwise(0))
df = df.withColumn("company_flag", when(lower(col("company_name")).rlike("(" + "|".join(company_indicators) + ")"), 1).otherwise(0))

# Add synthetic_score feature
df = df.withColumn("synthetic_score", col("email_flag") + col("phone_flag") + col("name_flag") +
                   col("dob_flag") + col("address_flag") + col("zip_flag") + col("company_flag"))

# Correct label mapping: real = 0, synthetic = 1
df = df.withColumn("indexed_label", when(col("label") == 0, 0).otherwise(1))

# Assemble features
assembler = VectorAssembler(
    inputCols=["email_flag", "phone_flag", "name_flag", "dob_flag", "address_flag", "zip_flag", "company_flag", "synthetic_score"],
    outputCol="features"
)

# Logistic Regression with adjusted threshold
lr = LogisticRegression(featuresCol="features", labelCol="indexed_label", predictionCol="prediction",
                        probabilityCol="probability", threshold=0.3)

# Build pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Split and train
train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)
model = pipeline.fit(train_df)

# Save model after training
model.write().overwrite().save("abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse/Files/ml_models/sythetic_customer_logistic_model")

# Apply model
predictions = model.transform(df)
predictions = predictions.withColumn("is_real_customer", when(col("prediction") == 0, lit("Y")).otherwise(lit("N")))

# Extract prediction and confidence
extract_confidence = udf(lambda v: f"{float(v[0]):.10f}", StringType())
predictions = predictions.withColumn("is_real_customer", when(col("prediction") == 0, lit("Y")).otherwise(lit("N")))
predictions = predictions.withColumn("confidence", extract_confidence(col("probability")))
predictions = predictions.withColumn("real_customer_probability", struct(col("is_real_customer"), col("confidence")))

# Convert confidence to score between 0.00 and 10.00
predictions = predictions.withColumn("real_customer_score", format_number((col("confidence") * 10), 2))

# Show results
display(predictions.select(
    "first_name", "last_name", "email", "phone", "date_of_birth", "address", "zip_code",
    "company_name", "is_real_customer", "confidence", "real_customer_score"
))


# Evaluate model on test set
predictions_test = model.transform(test_df)
evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="accuracy")
evaluator_precision = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="weightedPrecision")
evaluator_recall = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="weightedRecall")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="f1")
evaluator_auc = BinaryClassificationEvaluator(labelCol="indexed_label", rawPredictionCol="probability", metricName="areaUnderROC")
accuracy = evaluator_accuracy.evaluate(predictions_test)
precision = evaluator_precision.evaluate(predictions_test)
recall = evaluator_recall.evaluate(predictions_test)
f1_score = evaluator_f1.evaluate(predictions_test)
auc_score = evaluator_auc.evaluate(predictions_test)
print(f"Test Accuracy: {accuracy:.4f}")
print(f"Test Precision: {precision:.4f}")
print(f"Test Recall: {recall:.4f}")
print(f"Test F1 Score: {f1_score:.4f}")
print(f"Test AUC Score: {auc_score:.4f}")

# Visualize confusion matrix
y_true = predictions_test.select("indexed_label").rdd.flatMap(lambda x: x).collect()
y_pred = predictions_test.select("prediction").rdd.flatMap(lambda x: x).collect()
cm = confusion_matrix(y_true, y_pred)
ConfusionMatrixDisplay(confusion_matrix=cm).plot()
plt.title("Confusion Matrix")
plt.show()

# Visualize ROC curve
y_score = predictions_test.select("probability").rdd.map(lambda x: x[0][1]).collect()
fpr, tpr, _ = roc_curve(y_true, y_score)
roc_auc = auc(fpr, tpr)
plt.figure()
plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (area = {roc_auc:.2f})')
plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic')
plt.legend(loc="lower right")
plt.show()


#-------------------------------------------------------
# Score new customer data
#-------------------------------------------------------
# Load new customer data from Delta table
new_df = spark.read.format("delta").load("abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_bronze.Lakehouse/Tables/bronze_db/stg_cdtq_customer_0000001")

# Select and rename columns
new_df = new_df.select(
    col("first_name"),
    col("last_name"),
    col("email"),
    lit("").alias("phone"),  # placeholder since phone is missing
    col("dob").alias("date_of_birth"),
    col("street_address").alias("address"),
    col("postal").alias("zip_code"),
    col("company").alias("company_name")
)

#display(new_df)

# Create indicator flags
new_df = new_df.withColumn("email_flag", when(lower(col("email")).rlike("(" + "|".join(email_indicators) + ")"), 1).otherwise(0))
new_df = new_df.withColumn("phone_flag", when(col("phone").isin(phone_indicators), 1).otherwise(0))
new_df = new_df.withColumn("name_flag", when(lower(col("first_name")).rlike("(" + "|".join(name_indicators) + ")") |
                                      lower(col("last_name")).rlike("(" + "|".join(name_indicators) + ")"), 1).otherwise(0))
new_df = new_df.withColumn("dob_flag", when(col("date_of_birth").isin(dob_indicators), 1).otherwise(0))
new_df = new_df.withColumn("address_flag", when(lower(col("address")).rlike("(" + "|".join(address_indicators) + ")"), 1).otherwise(0))
new_df = new_df.withColumn("zip_flag", when(col("zip_code").isin(zip_indicators), 1).otherwise(0))
new_df = new_df.withColumn("company_flag", when(lower(col("company_name")).rlike("(" + "|".join(company_indicators) + ")"), 1).otherwise(0))

# Add synthetic_score
new_df = new_df.withColumn("synthetic_score",
    col("email_flag") + col("phone_flag") + col("name_flag") +
    col("dob_flag") + col("address_flag") + col("zip_flag") + col("company_flag"))

# Load trained model
model = PipelineModel.load("abfss://CDSA@onelake.dfs.fabric.microsoft.com/lk_cdsa_landing_zone.Lakehouse/Files/ml_models/sythetic_customer_logistic_model")

# Apply model
predictions = model.transform(new_df)

# Extract prediction and confidence
extract_confidence = udf(lambda v: f"{float(v[0]):.10f}", StringType())
predictions = predictions.withColumn("is_real_customer", when(col("prediction") == 0, lit("Y")).otherwise(lit("N")))
predictions = predictions.withColumn("confidence", extract_confidence(col("probability")))
predictions = predictions.withColumn("real_customer_probability", struct(col("is_real_customer"), col("confidence")))

# Convert confidence to score between 0.00 and 10.00
predictions = predictions.withColumn("real_customer_score", format_number((col("confidence") * 10), 2))

# Show results
display(predictions.select(
    "first_name", "last_name", "email", "phone", "date_of_birth", "address", "zip_code",
    "company_name", "is_real_customer", "confidence", "real_customer_score"
))

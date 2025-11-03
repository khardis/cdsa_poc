# ------------------------------------------------------------------------------------------------------------------
# mlUtil.py
# ------------------------------------------------------------------------------------------------------------------

__version__ = "ml_1.0"

from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, lower, lit, when, udf, format_number, round
from pyspark.sql.types import DoubleType

successCode = 0
errorCode = 1
InvalidArgsCode = 2
failedStr = 'FAILED'
completedStr = 'COMPLETED'

def score_customers(df, model_path, column_map=None):
    """
    Scores customer records using a trained logistic regression model and adds real_customer_score (0.00–10.00).
    
    Parameters:
    ----------
    df : DataFrame
        Input PySpark DataFrame containing staged customer data.
    model_path : str
        Path to the trained PipelineModel in Fabric (ABFSS path).
    column_map : dict (optional)
        Mapping of source columns to model feature names. Example:
        {
            "firstname": "first_name",
            "lastname": "last_name",
            "email": "email",
            "dob": "date_of_birth",
            "street_address": "address",
            "postal": "zip_code",
            "company": "company_name",
            "phone": "phone"
        }
    
    Returns:
    -------
    DataFrame
        Original DataFrame with an additional column: real_customer_score.
    """

    try:
        # Default column mapping if none provided
        if column_map is None:
            column_map = {
                "firstname": "first_name",
                "lastname": "last_name",
                "email": "email",
                "dob": "date_of_birth",
                "street_address": "address",
                "postal": "zip_code",
                "company": "company_name",
                "phone": "phone"
            }

        # Rename or create missing columns
        for src_col, target_col in column_map.items():
            if src_col in df.columns:
                df = df.withColumnRenamed(src_col, target_col)
            elif target_col not in df.columns:
                df = df.withColumn(target_col, lit(""))  # placeholder if missing

        # Indicator patterns
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

        # Add synthetic_score
        df = df.withColumn("synthetic_score",
            col("email_flag") + col("phone_flag") + col("name_flag") +
            col("dob_flag") + col("address_flag") + col("zip_flag") + col("company_flag"))

        # Load trained model
        model = PipelineModel.load(model_path)

        # Apply model
        predictions = model.transform(df)

        # Extract confidence for real class (index 0)
        extract_confidence = udf(lambda v: float(v[0]), DoubleType())
        predictions = predictions.withColumn("confidence", extract_confidence(col("probability")))

        # Convert confidence to score between 0.00 and 10.00
        predictions = predictions.withColumn("real_customer_score", round((col("confidence") * 10.0), 1).cast("float"))

        return predictions

    except Exception as e:
        errorStr = 'ERROR (score_customers)' + str(e)
        print(errorStr)
        raise

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, TimestampType

def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def load_raw_data(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform_data(input_data):
    # Convert timestamp columns to the correct data types
    data_etl = input_data.withColumn("transaction_ts", col("transaction_ts").cast(TimestampType()))
    data_etl = data_etl.withColumn("customer_enroll_ts", col("customer_enroll_ts").cast(TimestampType()))

    # Extract the rule index from the 'attr_name' column
    data_etl = data_etl.withColumn("rule_index", when(col("attr_name").contains("["),
                                                      col("attr_name").substr(col("attr_name").indexOf("[") + 1, 1)).otherwise(0))

    # Fill null values in 'rule_index' with 0
    data_etl = data_etl.fillna(0, subset=["rule_index"])

    # Convert 'attr_value' column to a numerical value (assuming it contains numeric data)
    data_etl = data_etl.withColumn("attr_value_num", col("attr_value").cast(DoubleType()))

    # Drop unnecessary columns
    data_etl = data_etl.drop("attr_name", "attr_value")

    return data_etl

def save_processed_data(data, output_path):
    data.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    # Step 1: Create a Spark session
    spark_session = create_spark_session("Risk_ETL")

    # Step 2: Load the raw data from the CSV file
    input_data_path = "Data_Eng_Spark_Test_07072023.csv"
    raw_data = load_raw_data(spark_session, input_data_path)

    # Step 3: Perform necessary transformations and data cleaning
    data_etl = transform_data(raw_data)

    # Step 4: Prepare the dataset for training
    # (Nothing to do here since all transformations are already done)

    # Step 5: Save the processed dataset for training
    output_data_path = "processed_data_for_training"
    save_processed_data(data_etl, output_data_path)

    # Step 6: Stop the Spark session
    spark_session.stop()

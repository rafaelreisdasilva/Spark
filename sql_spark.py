from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_max_consecutive_paid_installments(spark, file_path):
    # Create a Spark session
    spark_sess = SparkSession.builder \
        .appName("MaxPaidInstallments") \
        .getOrCreate()

    # Read the CSV file into a DataFrame
    df = spark_sess.read.csv(file_path, header=True, inferSchema=True)

    # Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("table")

    # Write the SQL query to find the maximum consecutive number of installments paid on time per user
    query = """
    WITH user_data AS (
      SELECT
        id_user,
        payment_date,
        ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY payment_date) - 
        ROW_NUMBER() OVER (PARTITION BY id_user, CASE WHEN status = 'Paid in time' THEN 0 ELSE 1 END ORDER BY payment_date) AS grp
      FROM table
    )
    SELECT
      id_user,
      MAX(CASE WHEN status = 'Paid in time' THEN grp ELSE 0 END) AS Max
    FROM user_data
    GROUP BY id_user
    ORDER BY id_user
    """

    # Execute the SQL query and get the result as a DataFrame
    result_df = spark_sess.sql(query)

    # Stop the Spark session
    spark_sess.stop()

    return result_df

if __name__ == "__main__":
    # File path of the CSV data
    csv_file_path = "DataEng_SQL_Test_20230707.csv"

    # Call the function to calculate the maximum consecutive number of installments paid on time per user
    result_dataframe = calculate_max_consecutive_paid_installments(SparkSession, csv_file_path)

    # Show the result
    result_dataframe.show()

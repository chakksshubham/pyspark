# sample intro spark code to check if the pyspark is working

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    import pandas as pd

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Spark App") \
        .master("local[*]") \
        .getOrCreate()

    # Create a simple DataFrame
    data = {
        "Serial_Id": [1, 2, 3],
        "Name": ["Alice", "Bob", "Cathy"],
        "Id": [101, 102, 103],
        "Age": [23, 25, 22],
        "City": ["New York", "Los Angeles", "Chicago"],
        "Country": ["USA", "USA", "USA"]
    }

    # Create a pandas DataFrame as spark method create dataframe only takes pandas df or list or tuple not
    df=pd.DataFrame(data)

    #convrting to spark df for parallel processing
    spark_df = spark.createDataFrame(df, ["Serial_Id","Name", "Id", "Age", "City", "Country"])

    # Show the DataFrame
    spark_df.show(5, False)

    # Stop the Spark session
    spark.stop()
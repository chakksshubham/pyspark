# the files informs about creating a spark session

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # create a spark session

    spark = SparkSession.builder \
        .appName("My first spark session") \
        .master("local[*]") \
        .getOrCreate()
    
    # it is a singleton object and each app can have only one active spark session 
    # because it is driver and we cannot have more than 1
    # builder method gives us a builder object which we can use to configure the spark session
    # appName method is used to set the name of the application

    # DO
    # THE
    # TRANSFORMATIONS
    # HERE
    print('Spark session created successfully')
    config = spark.sparkContext.getConf().getAll()  # get all the configurations of the spark session
    for key, value in config:
        print('{}: {}'.format(key, value))
    # print the configurations of the spark session
    print('The spark version is {}'.format(spark.version))  # print the version of the spark session


    spark.stop()  # stop the spark session
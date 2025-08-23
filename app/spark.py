from pyspark.sql import SparkSession

DB_URL = "jdbc:postgresql://pagila:5432/postgres"

spark = SparkSession.builder \
    .appName("MyPySparkApp") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver" 
}

df = spark.read.jdbc(url=DB_URL, table="film", properties=properties)

df.show()

spark.stop()
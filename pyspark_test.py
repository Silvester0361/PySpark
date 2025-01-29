# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# spark= SparkSession.builder.appName("test").getOrCreate()
# data = [
#     (1, "Alice", 25),
#     (2, "Bob", 35),
#     (3, "Catherine", 29),
#     (4, "David", 45),
#     (5, "Eve", 28)
# ]
#
# # Define the schema for the dataset
# schema = StructType([
#     StructField("ID", IntegerType(), True),
#     StructField("Name", StringType(), True),
#     StructField("Age", IntegerType(), True)
# ])
#
# # Create a DataFrame
# df = spark.createDataFrame(data, schema)
# df.show()
# spark.stop()
# from sysconfig import scheme

# from pyspark.shell import spark
# from pyspark.sql.functions import column

#
# from pyspark.sql import  SparkSession
#
# spark = SparkSession.builder.appName("sample").getOrCreate()
# sparkContext = spark.sparkContext
#
# rdd = sparkContext.parallelize([1,2,3,4,5,6])
# rddCollect = rdd.collect()
# last = rdd.take(6)[5]
#
# print("Number of partitions :"+str(rdd.getNumPartitions()))
# print("First element is "+str(rdd.first()))
# print("Last element is " +str(last))
# print(rdd.collect)
#
# partition_data = rdd.glom().collect()
#


# for i, partition in enumerate(partition_data):
#     print(f" {i} and {partition}")

# from pyspark.shell import spark
# Details = [("Silvester",23,"M"),("Roshan",20,"M"),("Susan",19,"F")]
#
# columns=["Name","Age","Gender"]
# df = spark.createDataFrame (data=Details,schema=columns)
# df.show()

from pyspark.sql import SparkSession
from pyspark.sql.types import *


schema = StructType([
    StructField("Full_name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True)
])


Details = [("Silvester", 23, "M"), ("Roshan", 20, "M"), ("Susan", 19, "F")]


spark = SparkSession.builder.appName('sample').getOrCreate()

df = spark.createDataFrame(data=Details, schema=schema)

df.printSchema()
df.show()



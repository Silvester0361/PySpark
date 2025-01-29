from pyspark.sql import  SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([StructField("Song Number",IntegerType(),True),
                     StructField("Singer",StringType(),True),
                     StructField("Date",StringType(),True),
                     StructField("Minutes",StringType(),True)])
spark = SparkSession.builder.appName("sample").getOrCreate()
df = spark.read.csv(path="D:\Python\Sample.csv" ,schema=schema, header= True)
df=df.withColumn("Date",to_date(col("Date"),"dd-MM-yyyy").alias("converted_date"))
df.show()
df.printSchema()

# new_date_df=df.select(col("Date"),to_date(col("Date"),"dd-MM-yyyy").alias("converted_date"))
# new_date_df.show()
# new_date_df.printSchema()





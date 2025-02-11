from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Example").getOrCreate()


data = [("Alice", 25), ("Bob", 16), ("Charlie", 30)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df1 = df.select(col("Name"),
                col("Age"),
                when(col("Age") >= 18, "Eligible").otherwise("Unknown").alias("Voting"))

df1.show()

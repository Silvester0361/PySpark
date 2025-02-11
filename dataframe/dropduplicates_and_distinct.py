from pyspark.sql import SparkSession
from pyspark.sql.functions import col
data = [("Alice", 25, "NY"),
        ("Bob", 30, "CA"),
        ("Alic", 25, "NY"),
        ("Charlie", 35, "TX"),
        ("Bob", 30, "CA")]

columns = ["Name", "Age", "State"]

sp = SparkSession.builder.appName("sample").getOrCreate()
d=sp.createDataFrame(data,columns)
d.distinct().show()
d.dropDuplicates(["Name"]).show()

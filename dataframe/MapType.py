from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,MapType,StructField,IntegerType

data = [("Silvester",{"age":23},{"Gender":"M"}), ("Roshan",{"age":20,"Gender":"M"}),
        ("Susan", {"age":23, "Gender": "F"})]

scheme = StructType([StructField("Name",StringType()),
                     StructField("Detail_1",MapType(IntegerType(),StringType()))])
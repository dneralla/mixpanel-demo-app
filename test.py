import pyspark
import pyspark.sql.functions as f

df = spark.read.table("samples.nyctaxi.trips")
df.write.format("json").mode("overwrite").save("gs://mixpanel-prod-1-warehouse-imports/testdatabricks/123/")

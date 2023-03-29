#Used industry standard TPCH orders and lineitem tables/data.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "aws_access_key")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "aws_secret_key")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

#df = spark.read.options(delimiter='|').csv('s3a://bucket/filepath', inferSchema=True, header=True)

#df.printSchema

#from pyspark.sql.types import StructField, StructType, StringType,LongType
#custom_schema_order = StructType([
#    StructField("o_orderkey", StringType(), True),
#    StructField("o_custkey", StringType(), True),
#    StructField("o_orderstatus", StringType(), True),
#        StructField("o_totalprice", StringType(), True),
#        StructField("o_orderdate", StringType(), True),
#        StructField("o_orderpriority", StringType(), True),
#        StructField("o_clerk", StringType(), True),
#        StructField("o_shippriority", StringType(), True),
#        StructField("o_comment", StringType(), True)
#])

custom_schema_lineitem = StructType([
    StructField("l_orderkey", StringType(), True),
    StructField("l_partkey", StringType(), True),
    StructField("l_suppkey", StringType(), True),
        StructField("l_linenumber", StringType(), True),
        StructField("l_quantity", StringType(), True),
        StructField("l_extendedprice", StringType(), True),
        StructField("l_discount", StringType(), True),
        StructField("l_tax", StringType(), True),
        StructField("l_returnflag", StringType(), True),
        StructField("l_linestatus", StringType(), True),
        StructField("l_shipdate", StringType(), True),
        StructField("l_commitdate", StringType(), True),
        StructField("l_receiptdate", StringType(), True),
        StructField("l_shipinstruct", StringType(), True),
        StructField("l_shipmode", StringType(), True),
        StructField("l_comment", StringType(), True)
])

df = spark.read.format("csv") \
    .options(delimiter='|') \
    .schema(custom_schema_lineitem) \
    .option("header", True) \
    .load("s3a://bucket/filepath")   

#df_ord = spark.read.format("csv") \
#    .options(delimiter='|') \
#    .schema(custom_schema_order) \
#    .option("header", True) \
#    .load("s3a://bucket/filepath")

##################### Write to Cassandra

#df_new = df.withColumn("Key_Col",concat(df.l_orderkey,df.l_linenumber))
#df_new.write.format("org.apache.spark.sql.Cassandra").option("table", "psthru_sf1").option("key.column", "Key_Col").save()


######################### Read from Cassandra
#df_read = spark.read.format("org.apache.spark.sql.cassandra").option("table", "psthru_sf1").option("key.column", "Key_Col").load()

#df_read.write.format('csv').option('header','true').save('s3a://bucket/fielpath')


########################### Lookup

target = df_ord.join(df_read,df_ord.o_orderkey==df_read.Key_Col,"left")
target.write.format('csv').option('header','true').save('s3a://bucket/fielpath')

from pyspark import SparkConf
from pyspark.sql import SparkSession
from shapely.wkb import loads

conf = SparkConf()
conf.setAll([
    ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0"),
    ("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"),
    ("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
])

spark = SparkSession.builder \
    .master("local[*]") \
    .config(conf=conf) \
    .appName("read-s3-with-spark") \
    .getOrCreate()

df = spark.read \
    .option("compression", "zstd") \
    .parquet("s3a://overturemaps-us-west-2/release/2024-12-18.0/theme=divisions/type=division/part-00000-ed644172-af5f-489e-9488-f690fd04c569-c000.zstd.parquet")

df_tr = df.filter(df["country"] == "TR")

geometry = loads(binary_data)

def wkb_to_wkt(binary_data):
    if binary_data:
        return loads(binary_data).wkt
    return None

wkb_to_wkt_udf = udf(wkb_to_wkt, StringType())

df_tr = df_tr.withColumn("poi", wkb_to_wkt_udf(df_tr["geometry"]))
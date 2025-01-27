from pyspark.sql import SparkSession, functions as F
from pyspark import SparkConf, SparkContext
import findspark


# /opt/manual/spark: this is SPARK_HOME path
try:
  findspark.init("/opt/spark")
  print("Spark path i ayarlandi")
except:
  print("Spark path i ayarlanamadi")

#aws configs
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
 
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.csv('s3://noaa-ufs-rnrmarine-pds/', inferSchema=True)

print(df)












"""spark = (
  SparkSession.builder
    .appName("ReaBinaryFilesFromPublicS3")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)"""


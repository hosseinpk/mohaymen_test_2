from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("reports").getOrCreate()


hconf = spark._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://minio:9000")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hconf.set("fs.s3a.access.key", "minioadmin")
hconf.set("fs.s3a.secret.key", "minioadmin123")

SRC = "s3a://data/REF_SMS/REF_CBS_SMS2.csv"
OUT = "s3a://reports/outputs"


df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(SRC))



df = (df
      .withColumn("ts", F.to_timestamp("RECORD_DATE", "yyyy/MM/dd HH:mm:ss"))
      .withColumn("revenue_toman", (F.col("DEBIT_AMOUNT_42").cast("double") / F.lit(10.0))))


df = df.withColumn(
    "bucket_15m",
    (F.from_unixtime(F.floor(F.unix_timestamp("ts") / 900) * 900).cast("timestamp"))
)


ref = spark.createDataFrame([(0, "Prepaid"), (1, "Postpaid")], ["PAYTYPE_515", "PayTypeName"])
df = df.join(ref, on="PAYTYPE_515", how="left")


r1 = (df.withColumn("day", F.to_date("ts"))
        .groupBy("day")
        .agg(F.sum("revenue_toman").alias("daily_revenue_toman")))


r2 = (df.groupBy("bucket_15m", "PAYTYPE_515", "PayTypeName")
        .agg(F.sum("revenue_toman").alias("revenue_toman"))
        .withColumnRenamed("bucket_15m", "RECORD_DATE"))


r3 = (df.groupBy("bucket_15m", "PAYTYPE_515", "PayTypeName")
        .agg(F.min("revenue_toman").alias("min_revenue_toman"),
             F.max("revenue_toman").alias("max_revenue_toman"))
        .withColumnRenamed("bucket_15m", "RECORD_DATE"))


r4 = (df.groupBy("bucket_15m", "PayTypeName")
        .agg(F.count(F.lit(1)).alias("Record_Count"),
             F.sum("revenue_toman").alias("revenue_toman"))
        .withColumnRenamed("bucket_15m", "RECORD_DATE"))


(r1.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUT}/01_daily_revenue"))
(r2.write.mode("overwrite").option("header", "true").csv(f"{OUT}/02_15m_revenue_by_paytype"))
(r3.write.mode("overwrite").option("header", "true").csv(f"{OUT}/03_15m_min_max_by_paytype"))
(r4.write.mode("overwrite").option("header", "true").csv(f"{OUT}/04_15m_count_and_revenue"))

spark.stop()

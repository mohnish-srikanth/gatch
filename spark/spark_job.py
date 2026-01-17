from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import shutil

# spark = SparkSession.builder.appName("gatch").config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate()

spark = (SparkSession.builder.appName("gatch") 
            .config("spark.master", "local[*]")  # Ensure local execution
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3')
            .config("spark.sql.adaptive.enable", "false")
            .config("spark.local.dir", "D:/gatch/spark_temp") # try to make it relative path to avoid unwanted folder creation in spark folder
            .config("spark.driver.host", "localhost")  # Explicitly set driver host
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.driver.extraClassPath", "C:/hadoop/bin")
            .config("spark.executor.extraClassPath", "C:/hadoop/bin")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.python.worker.reuse", "true")
            .config("spark.pyspark.python", "python")  # Specify the Python interpreter
            .config("spark.pyspark.driver.python", "python")  # Specify the Python interpreter for the driver
            .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

def readKafkaToDf(topic, schema):
    dfRaw = spark.read.format("kafka") \
                 .option("kafka.bootstrap.servers", "kafka:9092") \
                 .option("subscribe", topic) \
                 .load()

    dfJson = dfRaw.selectExpr("CAST(value AS STRING)")

    return dfJson.select(from_json(col("value"), schema).alias("data")).select("data.*")

standingsSchema = StructType([
    StructField("name", StringType()),
    StructField("position", StringType()),
    StructField("played", StringType()),
    StructField("win", StringType()),
    StructField("draw", StringType()),
    StructField("loss", StringType()),
    StructField("goalsfor", StringType()),
    StructField("goalsagainst", StringType()),
    StructField("goalsdifference", StringType()),
    StructField("total", StringType())
])

fixturesSchema = StructType([
    StructField("idEvent", StringType()),
    StructField("dateEvent", StringType()),
    StructField("strEvent", StringType()),
    StructField("strHomeTeam", StringType()),
    StructField("strAwayTeam", StringType()),
])

teamsSchema = StructType([
    StructField("idTeam", StringType()),
    StructField("strTeam", StringType()),
])

standingsDf = readKafkaToDf("plStandings", standingsSchema)
fixturesDf = readKafkaToDf("plFixtures", fixturesSchema)
teamDf = readKafkaToDf("plTeams", teamsSchema)

outputDir = "/data"

def overwriteParquet(df, path):
    if os.path.exists(path):
        shutil.rmtree(path)
    df.write.mode("overwrite").parquet(path)

overwriteParquet(standingsDf, os.path.join(outputDir, "pl_standings.parquet"))
overwriteParquet(fixturesDf, os.path.join(outputDir, "pl_fixtures.parquet"))
overwriteParquet(teamDf, os.path.join(outputDir, "pl_teams.parquet"))

spark.stop()
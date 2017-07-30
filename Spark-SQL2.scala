// Databricks notebook source
//Creating a Spark Session
import org.apache.spark.sql.SparkSession
val sparkSession = SparkSession.builder
  .master("local")
  .appName("my-spark-app")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()

// COMMAND ----------

val jsonData=spark.read.json("/mnt/databricks/people.json")

// COMMAND ----------

display(jsonData)

// COMMAND ----------

val rowTop=jsonData.map(row=>row.getString(0))


// COMMAND ----------

display(spark.sql("select * from ermployees"))

// COMMAND ----------

// To get a list of tables in the current database
val tables = spark.catalog.listTables()
display(tables)

// COMMAND ----------

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
val sparkSession = SparkSession.builder
  .master("local")
  .appName("my-spark-app")
  .config("spark.some.config.option", "config-value")
  .getOrCreate()
//val hiveCtx=new HiveContext(sc)
val rows=sparkSession.sql("SELECT * from ermployees")
val keys=rows.map(row=>row.getString(0))

//keys.saveAsParquetFile("/mnt/databricks/employee.parquet")


// COMMAND ----------

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._
sqlContext.registerFunction("strLenScala", (_: String).length)
val tweetLength = sqlContext.sql("SELECT strLenScala('name') FROM ermployess LIMIT 10")

// COMMAND ----------

// define the UDF
def convert2Years(date: String) = date.substring(7, 11)
// register to session
sparkSession.udf.register("convert2Years", convert2Years(_: String))
//val moviesDf = getMoviesDf // create dataframe usual way
val moviesDf.createOrReplaceTempView("ermployees") // 'movies' is used in sql below
val name = sqlContext.sql("SELECT convert2Years('name') FROM ermployess LIMIT 10")

// COMMAND ----------

val hiveCtx=new HiveContext(sc)
hiveCtx.sql("CREATE TEMPORARY FUNCTION name AS class.function")

// COMMAND ----------

SELECT SUM(user.favouritesCount), SUM(retweetCount), user.id FROM tweets
GROUP BY user.id

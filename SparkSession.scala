// Databricks notebook source
SparkSession can be created using a builder pattern. The builder will automatically reuse an existing SparkContext if not exits; and create an SparkContext if it does not exits. Configuration options set in the builder are automatically propagated over to Spark and Hadoop during I/O.


// COMMAND ----------

//created with builder pattern
import org.apache.spark.sql.SparkSession
val sparkSession=SparkSession.builder
.master("local[*]")
.appName("my-spark-app")
.config("spark.sql.warehouse.dir","target/spark-warehouse") //spark option configuration in the context
.getOrCreate()



// COMMAND ----------

in Spark RELP, the session is created automatically and store into the variable spark


// COMMAND ----------

spark

// COMMAND ----------

SparkSession is the unified entry point to read data, similar to the old SQLContext.read

// COMMAND ----------

val jsonData=spark.read.json("/mnt/databricks/people.json")

// COMMAND ----------

display(jsonData)

// COMMAND ----------

Running SQL queries, SparkSession can be execute SQL queries over data, getting the results back as a DataFrame (i.e DataSet[Row])

// COMMAND ----------

display(spark.sql("select * from person"))

// COMMAND ----------

Config options, SparkSession can be used to set runtime configuration options, which can be used to optimized behaviour or I/O hadoop behavior. //using SQl variable sustitution

// COMMAND ----------

spark.conf.set("spark.some.config","abcd")

// COMMAND ----------

spark.conf.get("spark.some.config")

// COMMAND ----------

// MAGIC %sql select "${spark.some.config}"

// COMMAND ----------

Working with metadata directly, sparksession includes "catalog" method that containg methods to work with the metastore (i.e data catalog). Methods return DataSets and can be use with the DataSet API

// COMMAND ----------

val tables=spark.catalog.listTables()

// COMMAND ----------

display(tables.filter(_.name contains "son"))

// COMMAND ----------

display(spark.catalog.listColumns("person"))

// COMMAND ----------

Accesing spark.sparkContext return underlying used to create RDD as well as managing cluster resources

// COMMAND ----------

spark.sparkContext

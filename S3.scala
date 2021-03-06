// Databricks notebook source
// Replace with your values
//
// NOTE: Set the access to this notebook appropriately to protect the security of your keys.
// Or you can delete this cell after you run the mount command below once successfully.

val AccessKey = ""
val SecretKey = ""
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = ""
val MountName = ""

dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")

// COMMAND ----------

val myRDD = sc.textFile("/mnt/databricks/README.md")
myRDD.count()

// COMMAND ----------

display(dbutils.fs.ls("/mnt/databricks"))

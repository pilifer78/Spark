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


// COMMAND ----------

dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")


// COMMAND ----------

import scala.util.parsing.json.JSON
val myrdd1=sc.wholeTextFiles("/mnt/databricks/employees")
val myrdd2=myrdd1.map(pair=>JSON.parseFull(pair._1).get.asInstanceOf[Map[String,String]])
for(record<-myrdd2.take(1))
  //getOrElse method to access a value or a default when no value is present.
  println(record.getOrElse("salary",null))

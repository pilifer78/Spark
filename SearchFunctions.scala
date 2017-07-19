// Databricks notebook source
//bug scala
class SearchFunctions(val query: String)
{
  def isMatch(s:String): Boolean={
    s.contains(query)
  }
  
  def getMatchesFunctionReference(rdd:RDD[String]): RDD[String]={
    //Problem: "isMatch" means "this.isMatch", so we pass all of "this"
    rdd.map(x=>x.split(query))
  }
  
  def getMatchesFieldReference(rdd: RDD[String]): RDD[String] = {
    //Problem: "query means "this.query", so we pass all of "this"
    rdd.map(x=>x.split(query))
  }
  
  def getMatchesNoReference(rdd: RDD[String]): RDD[String] = {
    //Safe: extract just the field we need into a local variable
    val query_r = this.query
    rdd.map(x=>x.split(query_r))
  }
  
  //If NotSerializableException ocurrs in Scala, a reference to a method or field in nonserializable
  //class is usually the problem
}

// COMMAND ----------

val queryRDD=sc.textFile("/mnt/databricks/README.md")


// COMMAND ----------



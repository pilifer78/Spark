// Databricks notebook source
val rdd=sc.parallelize(List("Hello world","This is","Pairs RDD","code","spark"))
val pairs=rdd.map(x=>(x.split(" ")(0),x))
pairs.collect()

// COMMAND ----------

// Creating PairRDD x with key value pairs
val rdd=sc.parallelize(Array(("a", 1), ("b", 1), ("a", 1),
     | ("a", 1), ("b", 1), ("b", 1),
     | ("b", 1), ("b", 1)), 3)
rdd.reduceByKey((x,y)=>x+y)
rdd.collect()
//Applaying reduceByKey operation on X
val y=rdd.reduceByKey((accum,n)=>accum+n)
y.collect()
//applying associative function
val x=y.reduceByKey(_+_)
x.collect()
//associative function separately
def sumFunc(accum:Int, n: Int) = accum+n
val z=rdd.reduceByKey(sumFunc)
z.collect()


// COMMAND ----------

val var1=rdd.groupByKey()
var1.collect()

// COMMAND ----------

val var2=rdd.mapValues(x=>x+1)
var2.collect()

// COMMAND ----------

val var3=rdd.flatMapValues(x=>(x to 5))
var3.collect()

// COMMAND ----------

val var4=rdd.sortByKey()
var4.collect()

// COMMAND ----------

val rdd2=sc.parallelize(Array(("d", 1), ("b", 1), ("e", 1),
     | ("a", 1), ("b", 1), ("d", 1),
     | ("h", 1), ("y", 1)), 3)
val var5=rdd2.subtractByKey(rdd)
var5.collect()

// COMMAND ----------

//Perform an inner join between two RDDS
val var6=rdd.join(rdd2)
var6.collect()

// COMMAND ----------

//Perform a join between two RDDs where the key must be present in the first RDD
val var7=rdd.rightOuterJoin(rdd2)
var7.collect()

// COMMAND ----------

//Perform a job between two RDDs where the key must be present in the other RDD
val var8=rdd.leftOuterJoin(rdd2)
var8.collect()

// COMMAND ----------

//Group data from both RDDs sharing the same key
val var9=rdd.cogroup(rdd2)
var9.collect()

// COMMAND ----------

val var10=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
var10.collect()

// COMMAND ----------

val input=sc.textFile("/mnt/databricks/README.md")
val words=input.flatMap(x=>x.split(" "))
val result=words.map(x=>(x,1))reduceByKey((x,y)=>x+y)
result.collect()

// COMMAND ----------

val input=sc.parallelize(Array(("d", 1), ("b", 2),("b", 1), ("e", 1),
     | ("a", 1), ("b", 1), ("d", 1),
     | ("h", 1), ("y", 1)), 3)
val result=input.combineByKey((v)=>(v,1),
(acc:(Int,Int),v)=>(acc._1 + v, acc._2 +1),
(acc1: (Int, Int), acc2: (Int, Int))=>(acc1._1 + acc2._1, acc1._2 + acc2._2)
).map{case (key,value)=>(key,value._1/value._2.toFloat)}
result.collectAsMap().map(println(_))

// COMMAND ----------

val data=Seq(("a",3), ("b",4), ("a",1))
val result1=sc.parallelize(data).reduceByKey((x,y)=>x+y) //default parallelism
result1.collect()
val result2=sc.parallelize(data).reduceByKey((x,y)=>x+y) //Custom parallelis
result2.collect()

// COMMAND ----------

storeAddress = {
(Store("Ritual"), "1026 Valencia St"), (Store("Philz"), "748 Van Ness Ave"),
(Store("Philz"), "3101 24th St"), (Store("Starbucks"), "Seattle")}
    
storeRating = {
(Store("Ritual"), 4.9), (Store("Philz"), 4.8))}

storeAddress.join(storeRating)=={
  (Store("Ritual"), ("1026 Valencia St", 4.9)),
(Store("Philz"), ("748 Van Ness Ave", 4.8)),
(Store("Philz"), ("3101 24th St", 4.8))
}

// COMMAND ----------

//bug
storeAddress.leftOuterJoin(storeRating) ==
{(Store("Ritual"),("1026 Valencia St",Some(4.9))),
(Store("Starbucks"),("Seattle",None)),
(Store("Philz"),("748 Van Ness Ave",Some(4.8))),
(Store("Philz"),("3101 24th St",Some(4.8)))}

storeAddress.rightOuterJoin(storeRating) ==
{(Store("Ritual"),(Some("1026 Valencia St"),4.9)),
(Store("Philz"),(Some("748 Van Ness Ave"),4.8)),
(Store("Philz"), (Some("3101 24th St"),4.8))}

// COMMAND ----------

val input=sc.parallelize(Array(("d", 1), ("b", 2),("b", 1), ("e", 1),
     | ("a", 1), ("b", 1), ("d", 1),
     | ("h", 1), ("y", 1)), 3)

val var1=input.countByKey()
println(var1)

val var2=input.collectAsMap()
println(var2)

val var3=input.lookup(2)
println(var3)

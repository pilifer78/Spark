// Databricks notebook source
import breeze.linalg.{Vector, DenseVector}
import breeze.linalg.{Vector, DenseVector}

// COMMAND ----------

case class DataPoint(x:Vector[Double], y:Double)
defined class DataPoint

// COMMAND ----------

def ParsePoint(x:Array[Double]): DataPoint={
  DataPoint(new DenseVector(x.slice(0,x.size-2)), x(x.size-1))
}
ParsePoint: (x:Array[Double])DataPoint

// COMMAND ----------

import org.apache.spark.SparkFiles
//val file=sc.addFile("/rute") //not apply in that case
val inFile=sc.textFile("/mnt/databricks/spam.data")

// COMMAND ----------

//convert the line to set of numbers in string format and then convert each of the number to a double
val nums=inFile.map(line=>line.split(' ').map(_.toDouble))


// COMMAND ----------

val points=nums.map(ParsePoint(_))

// COMMAND ----------

import java.util.Random

// COMMAND ----------

val rand=new Random(42)

// COMMAND ----------

points.firts()

// COMMAND ----------

var w=DenseVector.fill(nums.first.size-2){rand.nextDouble}

// COMMAND ----------

val iterations=100

// COMMAND ----------

import scala.math._

// COMMAND ----------

for(i<-1 to iterations){
  val gradient=points.map(p=>
  p.x * (1/ (1 +exp(-p.y*(w dot p.x))) -1) * p.y
  ).reduce(_+_)
  w-=gradient
}

// COMMAND ----------

w

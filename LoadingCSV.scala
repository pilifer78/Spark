// Databricks notebook source
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

// COMMAND ----------

val inputFile="/mnt/databricks/databricks/wikipedia_e_20140913.11.tsv"

// COMMAND ----------

val input=sc.textFile(inputFile)
val result=input.map{line=>
val reader=new CSVReader(new StringReader(line))
reader.readNext();
}

// COMMAND ----------

result.collect()

// COMMAND ----------

//loading cvs in full
case class Person(name: String, favoriteAnimal: String)
val input = sc.wholeTextFiles(inputFile)
val result = input.flatMap{ case (_, txt) =>
val reader = new CSVReader(new StringReader(txt));
reader.readAll().map(x => Person(x(0), x(1)))
}

// COMMAND ----------

employee.map(employee=>List(employee.name, employee.salary).toArray).mapPartitions{people => val stringWriter=new StringWriter();
val csvWriter.writeAll(employee.toList)
Iterator(stringWriter.toString)
}.saveAsTextFile(outFile)

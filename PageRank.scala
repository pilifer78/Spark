// Databricks notebook source
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
val part=new HashPartitioner(100)
//Assume that the neighbor list is saved as a Spark objectFile
val links = sc.objectFile[(String, Seq[String])]("/mnt/SecuenceFiles/SequenceFiles/SimpleSeqFile/part-00000")
.partitionBy(part)
.persist()

// COMMAND ----------

//Initialize each page's rank to 1.0; use MapValues, the resulting RDD has the same partitioner as links
var ranks=links.mapValues(v=>1.0)

// Run 10 iterations of PageRank
for (i <- 0 until 10) {
val contributions = links.join(ranks).flatMap {
case (pageId, (links, rank)) =>
links.map(dest => (dest, rank / links.size))
}
ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
}


// COMMAND ----------

ranks.collect()
//write out the final ranks
ranks.saveAsTextFile("/mnt/databricks/ranks")

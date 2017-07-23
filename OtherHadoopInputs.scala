// Databricks notebook source
//Loading key/value text input format
val file="/mnt/databricks/README.md"
val input=sc.hadoopFile[Text, Text, KeyValueTextInputFormat](file).map{case (x,y)=> (x.toString, y.toString)}

// COMMAND ----------

val inputFile="/mnt/databricks/employees.json"
//Loading LZO-compressed JSON with Elephant Bird
val input=sc.newAPIHadoopFile(inputFile, classOf[LzoJsonInputFormat], classOf[LongWritable], classOf[MapWritable], conf)
//Each MapWritable in input represents a JSON file

// COMMAND ----------

//protocol buffer definition
message Venue {
required int32 id = 1;
required string name = 2;
required VenueType type = 3;
optional string address = 4;
5 Sometimes called pbs or protobufs.
86
|
Chapter 5: Loading and Saving Your Dataenum VenueType {
COFFEESHOP = 0;
WORKPLACE = 1;
CLUB = 2;
OMNOMNOM = 3;
OTHER = 4;
}
}
message VenueResponse {
repeated Venue results = 1;
}

// COMMAND ----------

//Elephant Bird protocol buffer
val job = new Job()
val conf = job.getConfiguration
LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf);
val dnaLounge = Places.Venue.newBuilder()
dnaLounge.setId(1);
dnaLounge.setName("DNA Lounge")
dnaLounge.setType(Places.Venue.VenueType.CLUB)
val data = sc.parallelize(List(dnaLounge.build()))
val outputData = data.map{ pb =>
val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue]);
protoWritable.set(pb)
(null, protoWritable)
}
outputData.saveAsNewAPIHadoopFile(outputFile, classOf[Text],
classOf[ProtobufWritable[Places.Venue]],
classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)

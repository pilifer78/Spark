// Databricks notebook source
//load the user info from a Hadoop Sequence file on HDFS
//This distributes elements of userData by the HDFS block where they are found,
//and doen't provide Spark with any way of knowing in which partition a 
//particular userID is located

val sc=new SparkContext("myapp")
var userdata=sc.sequenceFile[UserID, UserInfo]("hdfs://...").persist() 

//Function called periodically to process a logfile of events in the past 5 minutes;

def processNewLogs(logFileName:String){
  val events=sc.sequenceFile[UserID, LinkInfo](logFileName)
  val joined=userData.join(events) //Rdd of UserId, UserInfo, LInkInfo pairs
  val offTopicVisits=joined.filter{
    case(userID, (userInfo, linkInfo)) => //Expand the tuple into its components
    !userInfo.topics.contains(linkInfo.topic)
  }.count()
  println("Number of visits to non-suscribed topics: " + offTopicVisits)
}

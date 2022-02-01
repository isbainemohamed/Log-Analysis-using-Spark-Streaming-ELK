package org.inpt.projet.scala

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object SparkStreamingProject_ISBAINE_JAWHAR_SNOUSSI extends App{

	//First we load the configuration set up at application.conf
  private val config = ConfigFactory.load()

	//Then we put the different fields at variables to use them later

  private val master = config.getString("spark.master")
  private val ApplicationName  = config.getString("spark.app.name")
  private val ESUser = config.getString("spark.elasticsearch.username")
  private val ESPass = config.getString("spark.elasticsearch.password")
  private val ESHost = config.getString("spark.elasticsearch.host")
  private val ESPort = config.getString("spark.elasticsearch.port")
	//we store the output mode at outputmode val 

  private val outputMode = config.getString("spark.elasticsearch.output.mode")
  private val OutputDestination = config.getString("spark.elasticsearch.data.source")
  private val checkpointLocation = config.getString("spark.elasticsearch.checkpoint.location")
  private val index = config.getString("spark.elasticsearch.index")
  private val docType = config.getString("spark.elasticsearch.doc.type")



  private val indexAndDocType = s"log/$docType"

  private val pathToJSONResource = config.getString("spark.json.resource.path")

  //creating SparkSession object with Elasticsearch configuration

  val sparkSession = SparkSession.builder()
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, ESUser)
    .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, ESPass)
    .config(ConfigurationOptions.ES_NODES, ESHost)
    .config(ConfigurationOptions.ES_PORT, ESPort)
    .master(master)
    .appName(ApplicationName)
    .getOrCreate()

	//Define the data(output) structure/schema : each object/line of the log file has 4 fields: 
	// HTTPCODE, SITE, URI and IP ADDRESS

  val schema = StructType(List(
    StructField("HTTPCODE", StringType, true),
    StructField("SITE", StringType, true),
    StructField("URI", StringType, true),
    StructField("IP", StringType, true)
  ))

	//Creating a StreamDataFrame
  val StreamDataFrame = sparkSession.readStream.option("delimiter", " ").schema(schema)
    .csv("/home/dba/Desktop/SparkStreaminProject/src/main/resources/data")

	//Write output strream into ElastcSearch usng StreamDataFrame.WriteStream method
  
StreamDataFrame.writeStream
    .outputMode(outputMode)
    .format(OutputDestination)
    .option("checkpointLocation", checkpointLocation)
    .start(indexAndDocType)
    .awaitTermination()



}

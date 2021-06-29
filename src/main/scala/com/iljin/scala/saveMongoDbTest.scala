package com.iljin.scala

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.streaming.Trigger

import java.util.Calendar

object  saveMongoDbTest {

  def main(args: Array[String]): Unit = {
    val spark =  SparkSession
      .builder
      .master("local[*]")
      .appName("saveMongoDbTest")
      //.config("spark.mongodb.input.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.sparkMachine111")
      //.config("spark.mongodb.output.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.sparkMachine111")
      .getOrCreate

    //val sc = spark.sparkContext

    val kafkaStreamDF =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "197.200.11.176:9093,197.200.11.177:9094")
      .option("subscribe", "dispMachine111")
      .option("startingOffsets", "earliest")
      .load()

    //kafkaStreamDF.show
    val dataSet =  kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .toDF()
    
    // sends to MongoDB once every 20 seconds
    val mongodb_uri = "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019"

    val mdb_name = "sharding"
    val mdb_collection = "sparkMachine111"

    val CountAccum: LongAccumulator = spark.sparkContext.longAccumulator("mongostreamcount")

    val structuredStreamForeachWriter: MongoDBForeachWriter = new MongoDBForeachWriter(mongodb_uri,mdb_name,mdb_collection,CountAccum)
    val query = dataSet.writeStream
      .foreach(structuredStreamForeachWriter)
      //.trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    while (!spark.streams.awaitAnyTermination(60000)) {
      println(Calendar.getInstance().getTime()+" :: mongoEventsCount = "+CountAccum.value)
    }

  }
}


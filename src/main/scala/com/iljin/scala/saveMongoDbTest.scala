package com.iljin.scala

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark

object  saveMongoDbTest {

  def main(args: Array[String]): Unit = {
    val spark =  SparkSession
      .builder
      .master("local[*]")
      .appName("dispMachine11")
      .getOrCreate


    val kafkaStreamDF =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "197.200.11.176:9093,197.200.11.177:9094")
      .option("subscribe", "dispMachine111")
      .option("startingOffsets", "earliest")
      .load()

    //kafkaStreamDF.show
    val dataSet =  kafkaStreamDF
      //.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(value AS STRING)")
      .toDF

    println("333444555Data Input Start!!333444555")

    //[STEP - 1] MongoDB Data Insert
    val toMongo = MongoSpark.save(dataSet.write.option("uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding").mode("overwrite"))

    //MongoSpark.write(dataSet).mode(SaveMode.Append).save() // (1) line

    //The Kafka stream should written and in this case we are writing it to console
    dataSet.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}


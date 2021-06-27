package com.iljin.scala

import org.apache.spark.sql.{Row, SparkSession}
import com.mongodb.spark.MongoSpark
import org.bson.Document

object  saveMongoDbTest {

  def main(args: Array[String]): Unit = {
    val spark =  SparkSession
      .builder
      .master("local[*]")
      .appName("saveMongoDbTest")
      .config("spark.mongodb.input.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.sparkDispMachine111")
      .config("spark.mongodb.output.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.sparkDispMachine111")
      .getOrCreate

    val sc = spark.sparkContext

    val kafkaStreamDF =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "197.200.11.176:9093,197.200.11.177:9094")
      .option("subscribe", "dispMachine111")
      .option("startingOffsets", "earliest")
      .load()

    //kafkaStreamDF.show
    val dataFrame =  kafkaStreamDF
      //.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(value AS STRING)")
      .toDF

    println("00000000000000000000011111")
//     dataFrame.show()
//   val rddData = dataFrame.rdd
    println("!1111111111111111111111222")

    MongoSpark.save(dataFrame.write.option("uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.sparkDispMachine111").mode("overwrite"))
//    sc.parallelize(dataFrame.map(Document.parse))
    //[STEP - 1] MongoDB Data Insert
//    val toMongo = MongoSpark.save(documents)

    //MongoSpark.write(dataSet).mode(SaveMode.Append).save() // (1) line

    //The Kafka stream should written and in this case we are writing it to console
//    dataSet.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//      .awaitTermination()
  }
}


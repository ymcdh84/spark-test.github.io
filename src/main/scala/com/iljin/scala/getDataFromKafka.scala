package com.iljin.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object getDataFromKafka {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("spark-kafka")
      .getOrCreate

    val kafkaStreamDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "ijdn01.iljincns.co.kr:6667,ijdn02.iljincns.co.kr:6667")
      .option("subscribe", "test-topic")
      .load()

    //kafkaStreamDF.show
    // Select data from the stream and write to file
    kafkaStreamDF.printSchema()
    kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    //val stream = kafkaStreamDF.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).outputMode("append").format("console").start().awaitTermination()
    val dataSet = kafkaStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val query = dataSet.writeStream.outputMode("append").format("console").start()

   query.awaitTermination()
  }
}


package com.iljin.scala

import org.apache.spark.sql.SparkSession

object  getDispMachine111 {

  def main(args: Array[String]): Unit = {

    val spark =  SparkSession
                .builder
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

    //[STEP - 1] 데이터 Console Print
    val query =  dataSet
                .writeStream
                .outputMode("append")
                .format("console")
                .start()
                .awaitTermination()

    println("3333333Data Input End!!333333344")
  }
}


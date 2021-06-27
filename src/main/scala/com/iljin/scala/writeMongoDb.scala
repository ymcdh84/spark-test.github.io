package com.iljin.scala

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document

object  writeMongoDb {

  def main(args: Array[String]): Unit = {
    val spark =  SparkSession
      .builder
      .master("local[*]")
      .appName("writeMonogoDb")
      .config("spark.mongodb.input.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.spark1")
      .config("spark.mongodb.output.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.spark1")
      .getOrCreate

    val sc = spark.sparkContext

    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))

    MongoSpark.save(documents) // Uses the SparkConf for configuration

  }
}


package com.iljin.scala

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object saveSeq {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.master("yarn").getOrCreate

    val conf = new SparkConf()

    conf.setMaster("yarn")
      .setAppName("testest")
      .set("spark.cleaner.referenceTracking", "false")
      .set("spark.cleaner.referenceTracking.blocking", "false")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "false")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "false")

    //val sc = new SparkContext(conf)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd_original : Array[Array[String]] = Array(
      Array("4580056797", "0", "2015-07-29 10:38:42", "0", "21", "12"),
      Array("4580056797", "0", "2015-07-29 10:38:42", "0", "12", "12"))

    val rdd : List[Array[String]] = rdd_original.toList

    val schemaString = "callId oCallId callTime duration calltype swId"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD to Rows.
    val rowRDD = rdd.map(p => Row(p: _*)) // using splat is easier
    // val rowRDD = rdd.map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5))) // this also works

    val df = spark.createDataFrame(sc.parallelize(rowRDD:List[Row]), schema)
    df.show
  }
}

package com.iljin.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object getDataFromHdfs {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder.master("yarn").getOrCreate

    val conf = new SparkConf()

    conf.setMaster("yarn")
      .setAppName("sspark")
      .set("spark.cleaner.referenceTracking", "false")
      .set("spark.cleaner.referenceTracking.blocking", "false")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "false")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "false")

    //val sc = new SparkContext(conf)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val pRDD  = sc.textFile("path_of_your_file")
    .flatMap(line => line.split(" "))
    .map{word=>(word,word.length)}
    
    //HDFS 파일 시스템에 있는 파일을 RDD 로드
    val lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-05/logstash-05.log")

    //[step-1] <[{> RowsData 앞 데이터 분리
    val rdd2 = lines.flatMap(_.split(",\"RowsData\":\\[\\{"))

    val frontLine = rdd2.flatMap(v => {
      if (v.contains("version")) {
        Some(v.slice(1, v.length))
      } else {
        None
      }
    })

    //[step-2] <}]> RowsData 뒷 데이터 분리
    val frontLineNone = rdd2.flatMap(v => {
      if (!v.contains("version")) {
        Some(v)
      } else {
        None
      }
    })

    val rdd3 = frontLineNone.flatMap(_.split("}],"))
    val backLine = rdd3.flatMap(v => {
      if (v.contains("timestamp")) {
        Some(v.slice(0, v.length - 1))
      } else {
        None
      }
    })

    //[step-3] <},{> RowsData 분리
    val rowsData = rdd3.flatMap(v => {
      if (!v.contains("timestamp")) {
        Some(v)
      } else {
        None
      }
    })

    //[step-4] frontLine,backLine Index append
    val frontLineIdxT = frontLine.zipWithIndex();
    val frontLineIdx = frontLineIdxT.map { case (k, v) => (v, k) };

    val backLineIdxT = backLine.zipWithIndex();
    val backLineIdx = backLineIdxT.map { case (k, v) => (v, k) };

    val frontBackLine = frontLineIdx.join(backLineIdx).sortByKey()

    //[step-5] RowsData Index append
    val rowsDataIdxT = rowsData.zipWithIndex();
    val rowsDataIdxT2 = rowsDataIdxT.map { case (k, v) => (v, k) }

    //[step-6] RowData 분리_index별 rowData
    var rowsDataIdx = rowsDataIdxT2.flatMapValues(_.split("},\\{"))

    //[step-7] frontBackLine 와 RowData 데이터 Join

    val resultData = frontBackLine.join(rowsDataIdx).sortByKey()
    val tupleList = resultData.map(v => {
                       (v._2._1._1 + "," + v._2._1._2 + "," + v._2._2)
                    })

    val tupleList2 = tupleList.map(
                      row => {
                        row.split(",")
                      })

    //[step-9] dataFrame 으로 변경
    val tutu = tupleList2.map(p => Row(p:_*))

    val schemaString = "callId oCallId callTime duration calltype swId"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val df = spark.createDataFrame(tutu , schema)
    df.show
  }
}


package com.iljin.scala

import org.apache.spark.{SparkConf, SparkContext}

object getDataFromHdfsTemp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("yarn")
      .setAppName("sspark")
      .set("spark.cleaner.referenceTracking", "false")
      .set("spark.cleaner.referenceTracking.blocking", "false")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "false")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "false")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //HDFS 파일 시스템에 있는 파일을 RDD 로드
    val lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-05/logstash-05.log")

    //[setp-1] <[{> RowsData 앞 데이터 분리
    val rdd2 = lines.flatMap(_.split(",\"RowsData\":\\[\\{"))

    val frontLine = rdd2.flatMap(v => {
      if(v.contains("version")){
        Some(v.slice(1, v.length))
      }else{
        None
      }
    })

    //[setp-2] <}]> RowsData 뒷 데이터 분리
    val frontLineNone = rdd2.flatMap(v => {
      if(!v.contains("version")){
        Some(v)
      }else{
        None
      }
    })

    val rdd3 = frontLineNone.flatMap(_.split("}],"))
    val backLine = rdd3.flatMap(v => {
      if(v.contains("timestamp")){
        Some(v.slice(0, v.length - 1))
      }else{
        None
      }
    })

    //[setp-3] <},{> RowsData 분리
    val rowsData = rdd3.flatMap(v => {
      if(!v.contains("timestamp")){
        Some(v)
      }else{
        None
      }
    })

    //[setp-4] frontLine,backLine 데이터 union
    val frontBackLine = frontLine.zip(backLine).zipWithIndex()
    val frontBackLineIdx = frontBackLine.map{case (k,v) => (v,k)}

    //[setp-5] RowsData Index append
    val rowsDataIdxT = rowsData.zipWithIndex();
    val rowsDataIdxT2 = rowsDataIdxT.map{case (k,v) => (v,k)}

    //[setp-6] RowData 분리
    var rowsDataIdx = rowsDataIdxT2.flatMapValues(_.split("},\\{"))

    //[setp-7] frontBackLine 와 RowData 데이터 Join
    val resultData = frontBackLineIdx.join(rowsDataIdx).sortByKey()

    resultData.collect.foreach(s => println(s))

    //resultData.collect.foreach(s => println(s))

    //    frontLine.collect.foreach(s => println(s))
    //    println("=================")
    //    println("=================")
    //    println("=================")
    //    backLine.collect.foreach(s => println(s))
  }
}


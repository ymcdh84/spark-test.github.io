package com.iljin.scala
import org.apache.spark.{SparkConf, SparkContext}

object getDataFromHdfs {
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

    //[step-1] <[{> RowsData 앞 데이터 분리
    val rdd2 = lines.flatMap(_.split(",\"RowsData\":\\[\\{"))

    val frontLine = rdd2.flatMap(v => {
      if(v.contains("version")){
        Some(v.slice(1, v.length))
      }else{
        None
      }
    })

    //[step-2] <}]> RowsData 뒷 데이터 분리
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

    //[step-3] <},{> RowsData 분리
    val rowsData = rdd3.flatMap(v => {
      if(!v.contains("timestamp")){
        Some(v)
      }else{
        None
      }
    })

    //[step-4] frontLine,backLine Index append
    val frontLineIdxT = frontLine.zipWithIndex();
    val frontLineIdx = frontLineIdxT.map{case (k,v) => (v,k)};

    val backLineIdxT = backLine.zipWithIndex();
    val backLineIdx = backLineIdxT.map{case (k,v) => (v,k)};

    val frontBackLine = frontLineIdx.join(backLineIdx).sortByKey()

    //[step-5] RowsData Index append
    val rowsDataIdxT = rowsData.zipWithIndex();
    val rowsDataIdxT2 = rowsDataIdxT.map{case (k,v) => (v,k)}

    //[step-6] RowData 분리
    var rowsDataIdx = rowsDataIdxT2.flatMapValues(_.split("},\\{"))

    //[step-7] frontBackLine 와 RowData 데이터 Join
    val resultData = frontBackLine.join(rowsDataIdx).sortByKey()
    var tupleList = resultData.map(v => {
                        val tp1 = v._2
                        val tp2 = tp1._1
                        tp2._1 + "," + tp2._2 + "," + tp1._2
                    })

    //tupleList.collect.foreach(s => println(s))
    //[step-8] hdfs에 저장
    tupleList.saveAsTextFile("/tmp/sub")
  }
}


package com.iljin.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object getDataFromHdfs {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("spark-test")
      .master("yarn")
      .config("spark.sql.warehouse.dir","/warehouse/tablespace/managed/hive")
      .enableHiveSupport()
      .getOrCreate

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //HDFS 파일 시스템에 있는 파일을 RDD 로드
    //val lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-23/cutparam-00.log") //rowData 앞 없는 데이터
    //val lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-21/cutparam-01.log")//rowData 앞/뒤 다 있는 데이터
    val lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-23/cutparam-08.log")//rowData 뒤 없는 데이터

    //[STEP-1] <[{> RowsData 앞 데이터 분리
    val rdd2 = lines.flatMap(_.split("\"RowsData\":\\[\\{"))

    val frontLine = rdd2.flatMap(v => {
      //if(v.slice(0, 2) == "{\""){
      if(v.length > 1 && v.slice(0, 2) == "{\""){
        Some(v.slice(1, v.length - 1))
      }else{
        None
      }
    })

    //[STEP-2] <}]> RowsData 뒷 데이터 분리
    val frontLineNone = rdd2.flatMap(v => {
        if (v.length > 1 && !(v.slice(0, 2) == "{\"")) {
          Some(v)
        } else {
          None
        }
    })

    val rdd3 = frontLineNone.flatMap(_.split("}]"))
    val backLine = rdd3.flatMap(v => {
      if (v.length > 1 && v.slice(v.length - 2, v.length) == "\"}") {
        Some(v.slice(0, v.length - 1))
      } else {
        None
      }
    })

    //[STEP-3] <},{> RowsData 분리
    val rowsData = rdd3.flatMap(v => {
      if (v.length > 1 && !(v.slice(v.length - 2, v.length) == "\"}")) {
        Some(v)
      } else {
        None
      }
    })

    //[STEP-4] frontLine,backLine Index append
    var frontLineIdxT: RDD[(String, Long)] = null
    var frontLineIdx: RDD[(Long, String)] = null

    if(frontLine.count > 0) {
      frontLineIdxT = frontLine.zipWithIndex()
      frontLineIdx = frontLineIdxT.map { case (k, v) => (v, k) }
    }

    var backLineIdxT: RDD[(String, Long)] = null
    var backLineIdx: RDD[(Long, String)] = null

    if(backLine.count > 0) {
      backLineIdxT = backLine.zipWithIndex()
      backLineIdx = backLineIdxT.map { case (k, v) => (v, k) }
    }

    var frontBackLine: RDD[(Long, String)] = null

    if(frontLine.count > 0 && backLine.count > 0){
      val frontBackLineT = frontLineIdx.join(backLineIdx).sortByKey()
      frontBackLine = frontBackLineT.map(v => {
                           (v._1 , v._2._1 + "," + v._2._2)
                        })
    }else if(frontLine.count > 0 ){
      frontBackLine = frontLineIdx;
    }else{
      frontBackLine = backLineIdx;
    }

    //[STEP-5] RowsData Index append
    val rowsDataIdxT = rowsData.zipWithIndex();
    val rowsDataIdxT2 = rowsDataIdxT.map { case (k, v) => (v, k) }

    //[STEP-6] RowData 분리_index별 rowData
    var rowsDataIdx = rowsDataIdxT2.flatMapValues(_.split("},\\{"))

    //[STEP-7] frontBackLine 와 RowData 데이터 Join
    var resultData : RDD[(Long,(String, String))]  = null
    if(frontLine.count > 0 && backLine.count > 0){
      resultData = frontBackLine.join(rowsDataIdx).sortByKey()
    }else if(frontLine.count > 0){
      resultData = frontLineIdx.join(rowsDataIdx).sortByKey()
    }else{
      resultData = backLineIdx.join(rowsDataIdx).sortByKey()
    }

    val tupleList = resultData.map(v => {
                       (v._2._1 + "," + v._2._2)
                    })

    //배열형태로 변경
    val tupleList2 = tupleList.map(
                      row => {
                        row.split(",")
                      })

    //키값 분리
    val keys = tupleList2.map(row => {
      row.map(row2 => {
        val a = row2.split("\":\"")
        a(0).replace("\"","")
      })
    })

    //데이터값 분리
    val values = tupleList2.map(row => {
      row.map(row2 => {
        val a = row2.split("\":\"")
        a(1).replace("\"","")
      })
    })

    //[STEP-8] dataFrame 으로 변경
    val tutu = values.map(p => Row(p:_*))

    // 컬럼명 추출
    val realKey = keys.first
    val schemaString = realKey.mkString(" ")
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // 데이터셋으로 변환
    val df = spark.createDataFrame(tutu , schema)
    df.show(tupleList.count().toInt, false)

    //[STEP-9] hdfs 저장
    df.write
      .format("com.databricks.spark.csv")
      .option("header","false")
      .save("/tmp/sub/json")

    df.createOrReplaceTempView("cutparam")
    spark.sql("select MachineID, CutID, Program, ItemValue, CompleteRate from cutparam where CompleteRate > 20").show

    //[STEP-10] table 저장
    df.write.mode(SaveMode.ErrorIfExists).saveAsTable("cutparam2")
    spark.sql("select * from cutparam").show
  }
}


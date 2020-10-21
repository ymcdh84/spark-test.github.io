package com.iljin.scala

import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object getDataFromHdfs {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf()
//
//    conf.setMaster("yarn")
//      .setAppName("sspark")
//      .set("spark.cleaner.referenceTracking", "false")
//      .set("spark.cleaner.referenceTracking.blocking", "false")
//      .set("spark.cleaner.referenceTracking.blocking.shuffle", "false")
//      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "false")
//  val sc = new SparkContext(conf)

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
    val lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-21/cutparam-01.log")

    //[STEP-1] <[{> RowsData 앞 데이터 분리
    val rdd2 = lines.flatMap(_.split(",\"RowsData\":\\[\\{"))

    val frontLine = rdd2.flatMap(v => {
      if(v.slice(0, 2) == "{\""){
        Some(v.slice(1, v.length))
      }else{
        None
      }
    })

    //[STEP-2] <}]> RowsData 뒷 데이터 분리
    val frontLineNone = rdd2.flatMap(v => {
        if (!(v.slice(0, 2) == "{\"")) {
          Some(v)
        } else {
          None
        }
    })

    val rdd3 = frontLineNone.flatMap(_.split("}],"))
    val backLine = rdd3.flatMap(v => {
      if (v.slice(v.length - 2, v.length) == "\"}") {
        Some(v.slice(0, v.length - 1))
      } else {
        None
      }
    })

    //[STEP-3] <},{> RowsData 분리
    val rowsData = rdd3.flatMap(v => {
      if (!(v.slice(v.length - 2, v.length) == "\"}")) {
        Some(v)
      } else {
        None
      }
    })

    //[STEP-4] frontLine,backLine Index append
    val frontLineIdxT = frontLine.zipWithIndex();
    val frontLineIdx = frontLineIdxT.map { case (k, v) => (v, k) };

    val backLineIdxT = backLine.zipWithIndex();
    val backLineIdx = backLineIdxT.map { case (k, v) => (v, k) };

    val frontBackLine = frontLineIdx.join(backLineIdx).sortByKey()

    //[STEP-5] RowsData Index append
    val rowsDataIdxT = rowsData.zipWithIndex();
    val rowsDataIdxT2 = rowsDataIdxT.map { case (k, v) => (v, k) }

    //[STEP-6] RowData 분리_index별 rowData
    var rowsDataIdx = rowsDataIdxT2.flatMapValues(_.split("},\\{"))

    //[STEP-7] frontBackLine 와 RowData 데이터 Join
    val resultData = frontBackLine.join(rowsDataIdx).sortByKey()

    val tupleList = resultData.map(v => {
                       (v._2._1._1 + "," + v._2._1._2 + "," + v._2._2)
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
//    df.show(tupleList.count().toInt, false)

    //[STEP-9] hdfs 저장
//    df.write
//      .format("com.databricks.spark.csv")
//      .option("header","false")
//      .save("/tmp/sub/json")

//    df.createOrReplaceTempView("cutparam")
//    spark.sql("select MachineID, CutID, Program, ItemValue, CompleteRate from cutparam where CompleteRate > 20").show

    //[STEP-10] table 저장
    df.write.mode(SaveMode.ErrorIfExists).saveAsTable("cutparam")
    //spark.sql("select * from cutparam").show

  }
}


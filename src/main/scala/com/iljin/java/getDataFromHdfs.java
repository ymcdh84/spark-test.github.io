package com.iljin.java;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class getDataFromHdfs {

    public static void main(String[] args) throws IOException, TException{
        SparkPi();
    }

    public static <ArrayListList> void SparkPi() throws IOException, TException {

        SparkConf conf = new SparkConf()
                .setAppName("spark")
                .setMaster("yarn")
                .set("spark.cleaner.referenceTracking","false")
                .set("spark.cleaner.referenceTracking.blocking","false")
                .set("spark.cleaner.referenceTracking.blocking.shuffle","false")
                .set("spark.cleaner.referenceTracking.cleanCheckpoints","false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //HDFS 파일 시스템에 있는 파일을 RDD 로드
        JavaRDD<String> lines = sc.textFile("hdfs://ijdn01.iljincns.co.kr:8020/user/logstash/dt=2020-10-05/logstash-05.log");
        //JavaRDD<String> lines2 = lines.flatMap(s -> Arrays.asList(s.split("RowsData")).iterator());

        JavaRDD<List<String>> lines2 = lines.map(line -> {

            List<String> dataList = new ArrayList<>();

            //[setp-1] <[{> RowsData 앞 데이터를 분리
            List<String> dataF = Arrays.asList(line.split("\\[\\{"));

            //[setp-2] <}]> RowsData 뒷 데이터를 분리
            List<String> dataS = Arrays.asList(dataF.get(1).split("}]"));

            //[setp-3] <},{> RowsData 건별 분리
            List<String> dataT = Arrays.asList(dataS.get(0).split("},\\{"));

            //[setp-4] 1,2번 데이터 union
            dataList.add(dataF.get(0));
            dataList.add(dataS.get(2));
            dataList.addAll(dataT);
            System.out.println("TESTTESTTEST22");

            System.out.println(dataList);

            //return sc.parallelize(dataList);
            return dataList;

            //[setp-5] 3번 데이터별로 4번 데이터 union
//            for(String ts : dataT){
//
//            }

            //JavaRDD<String> dataRow = null;
            //return dataF.iterator();
            //return dataList;
        });

//        //collect - RDD의 모든 원소를 모아 배열로 리턴
//        String sd = lines2.collect().get(1);
//        System.out.println(sd);

        for (List<String> lists : lines2.collect()) {
            for(String s : lists)
            System.out.println(s);
        }
    }
}


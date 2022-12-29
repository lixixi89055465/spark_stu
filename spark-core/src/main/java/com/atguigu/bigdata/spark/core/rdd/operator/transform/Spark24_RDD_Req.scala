package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")
    //    1516609143867 6 7 64 16
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    //3 将转换结构后的数据，进行分组聚合
    //((省份，广告),1)=> ((省份，广告),sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }
    //5 将转换结构后的数据根据省份进行分组
    //(省份,[ (广告A，sumA),(广告B,sumB)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    //6. 将分组后的数据组内排序（降序)，取前3名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resultRDD.collect().foreach(println)


    sc.stop()
  }
}



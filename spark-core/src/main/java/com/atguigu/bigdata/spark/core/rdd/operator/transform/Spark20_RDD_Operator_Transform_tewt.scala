package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform_tewt {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val dataRDD1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),("a", 4)))
//    val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(true)
        val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(false)
    sortRDD1.collect().foreach(println)
    sc.stop()
  }
}



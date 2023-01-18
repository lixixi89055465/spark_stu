package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 5), ("a", 2), ("a", 3), ("b", 4), ("b", 10)
    ))
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)
    rdd.groupBy(_._1)
    sc.stop()
  }
}



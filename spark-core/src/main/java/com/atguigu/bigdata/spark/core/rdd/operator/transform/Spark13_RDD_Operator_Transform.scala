package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - 双value 类型
    //    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),numSlices = 2)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4 ), numSlices = 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), numSlices = 2)
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    sc.stop()
  }
}



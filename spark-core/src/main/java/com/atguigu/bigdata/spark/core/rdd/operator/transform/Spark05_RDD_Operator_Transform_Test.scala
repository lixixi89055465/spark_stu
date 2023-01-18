package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), numSlices = 2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )
//    println(maxRDD.collect().sum)
    maxRDD.collect().foreach(println)

    sc.stop()
  }

}

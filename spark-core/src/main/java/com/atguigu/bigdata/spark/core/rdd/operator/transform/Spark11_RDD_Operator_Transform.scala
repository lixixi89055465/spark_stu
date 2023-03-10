package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - mapPartitions
//    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), numSlices = 3)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), numSlices = 2)
//        val newRdd: RDD[Int] = rdd.coalesce(numPartitions = 2)
    //val newRdd: RDD[Int] = rdd.coalesce(numPartitions = 2,shuffle = true)
    //        val newRdd: RDD[Int] = rdd.coalesce(numPartitions = 2, shuffle = true)
    // coalesce 方法默认不会将分区的数据打乱重新组合
    //coalesce 算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用
    // 所以如果要实现扩大分区的效果，需要使用shuffle操作
    // spark提供了一个简化的操作
    // 缩减分区:coalesce ,如果想过要数据均衡，可以采用shuffle
    // 扩大分区: repartition
    rdd.saveAsTextFile("output0")
    val newRdd: RDD[Int] = rdd.repartition(numPartitions = 3)
    newRdd.collect().foreach(println)
    newRdd.saveAsTextFile("output")
    sc.stop()
  }
}



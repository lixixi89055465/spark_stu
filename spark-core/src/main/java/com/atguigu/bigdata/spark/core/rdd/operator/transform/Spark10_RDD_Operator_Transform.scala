package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), numSlices = 3)
    //    val newRdd: RDD[Int] = rdd.coalesce(numPartitions = 2)
    rdd.collect().foreach(println)
//    val newRdd: RDD[Int] = rdd.coalesce(numPartitions = 2, shuffle = true)
    val newRdd: RDD[Int] = rdd.coalesce(numPartitions = 4, shuffle = true)
    newRdd.collect().foreach(println)
    // coalesce 方法默认不会将分区的数据打乱重新组合
    newRdd.saveAsTextFile("output")
    sc.stop()
  }
}



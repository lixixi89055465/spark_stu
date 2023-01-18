package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - map
    val rdd = sc.makeRDD(List(1, 2, 3, 4), numSlices = 2)
    val rddMap = rdd.mapPartitions(
      //      iter => {
      //        List(iter.max).iterator
      //      }
      iter => {
        println(iter)
        iter.foreach(println)
        iter
      }
    )
    rddMap.collect().foreach(println)

    sc.stop()
  }

}

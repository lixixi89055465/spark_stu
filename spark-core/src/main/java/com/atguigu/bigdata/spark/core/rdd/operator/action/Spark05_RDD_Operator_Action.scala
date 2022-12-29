package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 4, 4, 4), numSlices = 2)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 11)
    ))
    //TODO 行动算子
    rdd.saveAsTextFile("output")
    rdd.saveAsTextFile("output1")
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }
}



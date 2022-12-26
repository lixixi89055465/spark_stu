package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - mapPartitions
    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala", "hadoop"), numSlices = 2)

    val groupRDD = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)
    sc.stop()
  }

}

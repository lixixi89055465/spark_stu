package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - sortby
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), numSlices = 2)
    val newRdd = rdd.sortBy(t => t._1.toInt, ascending = false)
    newRdd.collect().foreach(println)
    sc.stop()
  }
}



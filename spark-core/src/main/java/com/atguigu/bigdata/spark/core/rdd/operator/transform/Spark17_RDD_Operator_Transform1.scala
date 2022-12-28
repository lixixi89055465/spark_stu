package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), numSlices = 2)
    rdd.aggregateByKey(zeroValue = 5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)
    rdd.aggregateByKey(zeroValue = 0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)
    rdd.aggregateByKey(zeroValue = 0)(
      math.max(_, _),
      _ + _
    ).collect().foreach(println)

    sc.stop()
  }
}



package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 5), ("a", 2), ("a", 3), ("a", 4)
    ), numSlices = 2)
    rdd.aggregateByKey(zeroValue = 0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    sc.stop()
  }
}



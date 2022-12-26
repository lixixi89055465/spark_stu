package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("datas/apache.log")
    val value: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )
    value
    sc.stop()
  }

}

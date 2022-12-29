package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println("sum = :" + sum)
    sc.stop()


  }

}

package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    //spark 框架提供了更多的功能，可以将分组和聚合用一个方法实现
    val wordToCount = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}

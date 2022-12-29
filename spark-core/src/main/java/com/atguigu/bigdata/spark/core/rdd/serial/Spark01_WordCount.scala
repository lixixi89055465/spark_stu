package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)
    sc.stop()
  }

}

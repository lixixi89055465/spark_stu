package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep{
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("#"*100)
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(lines.dependencies)
    println("#"*100)
    val wordToOne = words.map(
      word => (word, 1)
    )
    println(lines.dependencies)
    println("#"*100)
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)
    sc.stop()
  }
}

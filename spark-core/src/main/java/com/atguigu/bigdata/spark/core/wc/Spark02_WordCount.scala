package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1,t2)=>{
            (t1._1,t1._2+t2._2)
          }
        )
      }
    }
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}

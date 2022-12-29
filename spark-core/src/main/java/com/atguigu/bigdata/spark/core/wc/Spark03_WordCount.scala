package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {


  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    wordcount8(sc)

    sc.stop()
  }

  //foldByKey
  def wordcount9(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    mapWord.reduce(
      (map1, map2) => {
        map1
      }
    )

  }

  //foldByKey
  def wordcount8(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
    println(wordCount)
  }

  //foldByKey
  def wordcount7(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val stringToLong: collection.Map[String, Long] = wordOne.countByKey()
    println(stringToLong)
  }

  def wordcount1(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  def wordcount2(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  //foldByKey
  def wordcount4(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
  }

  //foldByKey
  def wordcount5(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
  }

  //foldByKey
  def wordcount6(sc: SparkContext) = {
    var rdd = sc.makeRDD(List("hello scala", "Hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val value: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x + y,
    )
    value.collect().foreach(println)
  }

}

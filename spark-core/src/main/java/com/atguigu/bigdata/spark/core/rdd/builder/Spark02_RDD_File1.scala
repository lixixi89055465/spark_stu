package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //从文件中创建RDD,将文件中的数据作为处理的数据源
    // textFile ： 以行为单位来读取数据
    // wholeTextFiles :以文件为单位读取数据
    val rdd = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)
    sc.stop()


  }

}

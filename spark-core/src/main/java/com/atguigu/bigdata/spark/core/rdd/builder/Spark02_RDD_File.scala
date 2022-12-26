package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    //从文件中创建RDD,将文件中的数据作为处理的数据源
    //path路径默认以当前环境的根路径未基准。可以写绝对路径，也可以写相对路径
    //path路径可以是文件的具体路径，也可以是目录名称
    //    val rdd: RDD[String] = sc.textFile("datas/1.txt")
    //    val rdd: RDD[String] = sc.textFile("datas")
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")
//    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/test.txt")
    rdd.collect().foreach(println)
    sc.stop()


  }

}
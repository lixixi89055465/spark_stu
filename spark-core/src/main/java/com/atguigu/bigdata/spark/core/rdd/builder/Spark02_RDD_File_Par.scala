package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // minPartitions 最小分区数量
    val rdd = sc.textFile("datas/1.txt", minPartitions = 3)

    rdd.saveAsTextFile("output")


    sc.stop()


  }

}

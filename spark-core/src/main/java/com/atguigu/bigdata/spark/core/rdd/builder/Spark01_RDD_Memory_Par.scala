package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(sparkConf)
    //RDD的并行度 & 分区
    //makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4), numSlices = 2)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")
    //TODO 关闭环境
    sc.stop()


  }

}

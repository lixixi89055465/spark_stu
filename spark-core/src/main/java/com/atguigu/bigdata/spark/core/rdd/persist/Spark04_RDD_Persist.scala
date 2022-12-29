package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
    sc.setCheckpointDir("cp")
    val list = List("Hello Scala", "Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    flatRDD.collect().foreach(println)
    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("@@@@@@@@@@@@@")
      (word, 1)
    })
    //数据持久化
    //    mapRDD.cache()
    //    mapRDD.persist(StorageLevel.DISK_ONLY)
    //    mapRDD.persist()
    //checkpoint 需要落盘，需要指定检查点保存路径
    //检查点路径保存的文件，当作业执行完毕后，不会被删除
    //一般保存路径都是在分布式存储系统中： HDFS

    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    println("***********************************************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

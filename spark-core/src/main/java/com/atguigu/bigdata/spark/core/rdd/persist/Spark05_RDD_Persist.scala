package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Persist {
  def main(args: Array[String]): Unit = {
    //cache :将数据临时存储在内存中进行重用
    // persisit: 将数据临时存储在磁盘文件中进行数据重用,
    //        涉及到磁盘IO，性能较低，但是数据安全，
    //        如果作业执行完毕，临时保存的数据文件就会丢失
    //        为了保存数据安全，所以一般情况下，会独立执行作业
    //         为了能够提高效率，一般情况下，是需要和cache联合使用

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

    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    println("***********************************************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //TODO 行动算子
    //    所谓的行动算子，其实就是触发作业job执行的方法
    //底层代码调用的是环境对象的runJob方法
    //collect :方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组

//    val i: Int = rdd.reduce(_ + _)
//    println(i)
    //    val ints: Array[Int] = rdd.collect()
    //    println(ints.mkString(","))
    //count: 数据源中数据的个数
//    val cnt: Long = rdd .count()
//    println(cnt)
    //first : 获取 数据源中第一个数据
//    val first: Int = rdd.first()
//    println(first)
//    val ints: Array[Int] = rdd.take(3)
//    println(ints.mkString(","))
    //takeOrdered: 数据排序后，取N个数据
    val rdd1: RDD[Int] = sc.makeRDD(List(4,3,2,12))
    val ints: Array[Int] = rdd1.takeOrdered(3)
    println(ints.mkString(","))



    sc.stop()
  }
}



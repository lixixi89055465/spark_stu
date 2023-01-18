package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3),
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("c", 5), ("a", 6), ("d", 10),
    ))
    //两个不同数据源的数据，相同的key的value 会连接在一起，形成元组
    // 如果两个数据源中的key没有匹配上，那么数据不会出现在结果中
    //如果两个数据源中key有多个相同的，会一次匹配，可能出现笛卡尔乘积
    //    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    val joinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    joinRDD.collect().foreach(println)


    sc.stop()
  }
}



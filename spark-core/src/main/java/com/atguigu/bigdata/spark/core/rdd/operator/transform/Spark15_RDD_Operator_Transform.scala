package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))
    //reduceByKey ： 相同的key 的数据进行value 数据的聚合操作
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x= ${x}, y= ${y}")
      x + y
    })
    reduceRDD.collect().foreach(println)

    sc.stop()
  }
}



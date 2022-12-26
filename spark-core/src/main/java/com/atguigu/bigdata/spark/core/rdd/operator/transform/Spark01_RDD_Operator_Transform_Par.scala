package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
//    val rdd = sc.makeRDD(List(1, 2, 3, 4),numSlices = 1)
    val rdd = sc.makeRDD(List(1, 2, 3, 4),numSlices = 2)
    val mapRDD = rdd.map(
      num => {
        println(">>>>>>> " + num)
        num * 2
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        println("############## " + num)
        num
      }
    )
    mapRDD1.collect().foreach(println)
    sc.stop()
  }

}

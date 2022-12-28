package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform_Test{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - mapPartitions
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val timeRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        (hour, 1)
      }
    ).groupBy(_._1)
    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }
    timeRDD.collect().foreach(println)
    sc.stop()
  }

}

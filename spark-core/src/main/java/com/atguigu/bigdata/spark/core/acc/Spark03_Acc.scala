package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统累加器
    //Spark 默认就提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    val mapRDD: RDD[Int] = rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )
    //获取累加器的值
    //少加：转换算资中调用累加器，如果没有行动算子的话， 那么不会执行
    //多加 ：转换算资中调用累加器，如果没有行动算资的话，
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)
    sc.stop()


  }

}

package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统累加器
    //Spark 默认就提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )
    println(sumAcc)
    println(sumAcc.value)
    sc.stop()


  }

}

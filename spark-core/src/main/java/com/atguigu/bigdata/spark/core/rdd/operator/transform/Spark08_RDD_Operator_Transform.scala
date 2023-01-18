package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 - mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //sample 算资需要传递三个参数
    //1 .第一个参数表示，抽取数据后，是否将数据返回 true (放回)
    //2. 第二个参数表示，数据源中每条数据被抽取的概率
    //3 第三个参数表示，抽取数据时随机算法的种子
    //            如果不传递第三个参数，那么使用的是当前系统的时间

    for (i <- 1 until 5) {
      println(rdd.sample(
//        withReplacement = false,
        withReplacement = true,
        fraction = 0.4,
        //      seed = 2
      ).collect().mkString(","))
    }
    sc.stop()
  }
}



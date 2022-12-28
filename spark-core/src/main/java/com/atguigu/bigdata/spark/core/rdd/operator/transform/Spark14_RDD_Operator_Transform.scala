package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), numSlices = 2)
    //RDD => PairRDDFunctions
    //隐式转换（二次编译）
    //partitionBy根据指定的分区规则对数据进行重分区
    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1))
    mapRdd.partitionBy(new HashPartitioner(partitions = 2)) //
      .saveAsTextFile("output")
    sc.stop()
  }
}



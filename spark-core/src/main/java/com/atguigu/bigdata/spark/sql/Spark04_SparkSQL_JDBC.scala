package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark04_SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._


    //TODO  关闭环境
    spark.close()
  }

}

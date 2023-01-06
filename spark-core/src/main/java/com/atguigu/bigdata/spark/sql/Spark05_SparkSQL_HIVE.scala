package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark04_SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    //使用SparkSQL 连接外置的HIVE
    //1. 拷贝Hive-size.xml文件到classpath 下
    //2. 启用Hive 的支持
    //3. 增加对应的依赖关系(包含MySQL)


    //TODO  关闭环境
    spark.close()
  }

}

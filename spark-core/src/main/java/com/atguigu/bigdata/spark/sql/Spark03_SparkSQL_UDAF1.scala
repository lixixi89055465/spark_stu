package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}

object Spark03_SparkSQL_UDAF1 {

  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    //    spark.udf.register("prefixName", (name: String) => {
    //      "Name:"  + name
    //    })
    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

    //早期版本中，spark 不能再sql 中使用强类型UDAF操作
    // SQL & DSL
    // 早期的UDAF强类型聚合函数使用DSL语法操作

    //    spark.sql("select age,prefixName(username) from user").show
    spark.sql("select ageAvg(age) from user").show
    val ds: Dataset[User] = df.as[User]
    //将UDAF函数转换为查询的列对象
    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn
    ds.select(udafCol).show

    //TODO  关闭环境
    spark.close()
  }

  /**
    * 自定义聚合函数：计算年龄的平均值
    * 1.继承 org.apache.spark.sql.expressions.Aggregator 定义泛型
    * IN: 输入的数据类型Long
    * BUF: 缓冲区的数据类型 Buff
    * OUT: 输出的数据类型Long
    * 2.重写方法
    *
    */
  case class User(username: String, age: Long)

  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    //z & zero: 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)

    }

    //根据输入的数据更新缓冲区的数据
    //    override def reduce(b: Buff, in: Long): Buff = {
    //      b.total = b.total + in
    //      b.count = b.count + 1
    //      b
    //    }

    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
      b.count = b.count + 1
      b
    }

    //合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1

    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong

  }

}

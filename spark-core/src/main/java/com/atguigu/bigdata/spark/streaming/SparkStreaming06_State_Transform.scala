package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    //transform 方法可以将底层RDD获取到后进行操作
    //1. DStream 功能不完善
    //2. 需要代码周期性的执行

    //Code: Driver端
    val newDS: DStream[String] = lines.transform(
      rdd => {
        //Code : Driver端，（周期性执行)
        rdd.map(
          str => {
            //Code :Executor 端
            str
          }
        )
      }
    )
    val newDS1: DStream[String] = lines.map(
      data => {
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}

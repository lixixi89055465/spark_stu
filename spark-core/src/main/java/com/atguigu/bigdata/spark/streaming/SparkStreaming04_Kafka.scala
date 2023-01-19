package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext 创建时，需要传递两个参数
    //第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //第二个参数表示批量处理的周期(采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //3.读取 Kafka 数据创建 DStream(基于 Receive 方式)
//    val kafkaDStream: ReceiverInputDStream[(String, String)] =
//      KafkaUtils.createStream(ssc,
//        "linux1:2181,linux2:2181,linux3:2181",
//        "atguigu",
//        Map[String, Int]("atguigu" -> 1))
//

    ssc.start()
    ssc.awaitTermination()

  }
}

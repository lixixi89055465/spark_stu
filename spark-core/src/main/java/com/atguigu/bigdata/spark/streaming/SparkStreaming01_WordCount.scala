package com.atguigu.bigdata.spark.streaming

<<<<<<< HEAD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
=======
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
>>>>>>> e521ad49d0ffbe767dc37fe99b2d9046c0cc91bf

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext 创建时，需要传递两个参数
    //第一个参数表示环境配置
<<<<<<< HEAD
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //第二个参数表示批量处理的周期(采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //TODO 逻辑处理
    //获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", port = 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordToOne: DStream[(String, Int)] = words.map((_, 1))
    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    wordToCount.print()


    //TODO 关闭环境
    //由于SparkStreaming 采集器是长期执行的任务，所以不能直接关闭
    //如果main 方法执行完毕，应用程序也会自动结束。所以不能让main 执行完毕
    //    ssc.stop()
    //1.启动采集器
    ssc.start()
    //2.等待采集器的关闭
    ssc.awaitTermination()

  }
=======
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //TODO逻辑处理
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map((_, 1))
    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    wordToCount.print()

    //TODO关闭环境
    //由于sparkStreaming 采集器是长期执行的任务，所以不能直接关闭
    //    ssc.stop()
    //1. 启动采集器
    ssc.start()
    //2.等待采集器关闭
    ssc.awaitTermination()


  }

>>>>>>> e521ad49d0ffbe767dc37fe99b2d9046c0cc91bf
}

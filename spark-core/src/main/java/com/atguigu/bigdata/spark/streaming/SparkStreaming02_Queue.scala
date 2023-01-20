package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
<<<<<<< HEAD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
=======
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
>>>>>>> e521ad49d0ffbe767dc37fe99b2d9046c0cc91bf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

<<<<<<< HEAD
object SparkStreaming02_Queue {
=======
object SparkStreaming01_WordCount {
>>>>>>> e521ad49d0ffbe767dc37fe99b2d9046c0cc91bf
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext 创建时，需要传递两个参数
    //第一个参数表示环境配置
<<<<<<< HEAD
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //第二个参数表示批量处理的周期(采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    reducedStream.print()
    for (i <- 1 to 5) {
      print(i)
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.start()
    ssc.awaitTermination()

  }
=======
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //3. 创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //4. 创建QueueInputDStream

    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //6.打印结果
    reducedStream.print()
    //7 启动任务
    ssc.start()
    //8 . 循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

>>>>>>> e521ad49d0ffbe767dc37fe99b2d9046c0cc91bf
}

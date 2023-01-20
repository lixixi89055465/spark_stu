package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //    ssc.checkpoint("cp")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    wordToOne.print()
    ssc.start()
    new Thread(
      new Runnable {
        override def run(): Unit = {
          //优雅的关闭
          //计算节点不再接受新的数据，而是将现有的数据处理完毕，然后关闭
          //MySql: Table (stopSpark)
          //Redis: Data(K-V)
          //ZK : /stopSpark
          //HDFS  : /stopSpark
          /*
          while (true) {
            if (true) {
              // 获取SparkStreaming 状态
              val state: StreamingContextState = ssc.getState()
              if (state == StreamingContextState.ACTIVE) {
                ssc.stop(true, true)
              }
            }
            Thread.sleep(5000)
          }
           */
          Thread.sleep(5000)
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
          System.exit(0)
        }
      }
    ).start()
    ssc.awaitTermination()
  }

}

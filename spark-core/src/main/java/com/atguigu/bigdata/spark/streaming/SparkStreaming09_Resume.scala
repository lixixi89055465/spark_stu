package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      //    ssc.checkpoint("cp")
      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
      ssc
    })
    ssc.checkpoint("cp")


    ssc.start()
    ssc.awaitTermination()
  }

}

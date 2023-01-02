package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCategoryTop10Analysis2 {
  def main(args: Array[String]): Unit = {
    //TODO : Top 10 热门品类
    var sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)
    //Q:actionRDD 重复使用
    //Q: cogroup 性能可能较低

    //1.读取原始日志数据
    val actionRDD = sc.textFile("datas/user_visit_action.txt")
    //2 将数据转换结构
    // 点击的场合：（品类ID，(1,0,0))
    //下单的场合：( 品类ID，（0,1,0))
    //支付的场合: (品类ID,(0,0,1))
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          //点击的场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          //下单的场合
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          //支付的场合
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    //3 将相同的品类ID的数量进行分组聚合
    //(品类ID,(点击数量，下单数量，支付数量))
    val analysisRDD = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //4 将 统计结果根据数量进行降序处理，取前10名
    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)
    resultRDD.foreach(println)
    sc.stop()

  }

}

package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)
    val actionRDD = sc.textFile("datas/user_visit_action.txt")
    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong,
        )
      }
    )
    actionRDD.cache()
    //TODO 对指定的页面连续跳转进行统计
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)
    println("1" * 100)
    println(ids.tail)
    println(okflowIds)
    //TODO 计算分母
    val pageidToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => {
        ids.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap
    //TODO 计算分子
    //根据sessionID 进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    //分组后，根据访问时间进行排序 (升序）

    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        //将不合法的页面跳转进行过滤
        pageflowIds.filter(
          t => {
            okflowIds.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )
    //((1,2),sum)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    //((1,2),sum)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)
    //TODO计算单挑转换率
    //分子除以分母有
    dataRDD.foreach {
      case ((pageid1, pageid2), sum) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 1L)
        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + (sum.toDouble / lon))

      }
    }


    sc.stop()
  }

  case class UserVisitAction(
                              date: String, // 用户点击行为的日期
                              user_id: Long, // 用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, // 某个页面的 ID
                              action_time: String, // 动作的时间点
                              search_keyword: String, // 用户搜索的关键词
                              click_category_id: Long, // 某一个商品品类的 ID
                              click_product_id: Long, // 某一个商品的 ID
                              order_category_ids: String, // 一次订单中所有品类的 ID 集合
                              order_product_ids: String, // 一次订单中所有商品的 ID 集合
                              pay_category_ids: String, // 一次支付中所有品类的 ID 集合
                              pay_product_ids: String, // 一次支付中所有商品的 ID 集合
                              city_id: Long
                            )

}

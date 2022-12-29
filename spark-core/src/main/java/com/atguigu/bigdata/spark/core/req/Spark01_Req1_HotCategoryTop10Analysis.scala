package com.atguigu.bigdata.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    var sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)
    //1.读取原始日志数据
    //2. 统计品类的点击数量
    //3. 统计品类的下单数量
    //4. 统计品类的支付数量
    //5. 将品类进行排序

    sc.stop()
  }

}

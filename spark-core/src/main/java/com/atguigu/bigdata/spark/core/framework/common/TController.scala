package com.atguigu.bigdata.spark.core.framework.common

import org.apache.spark.{SparkConf, SparkContext}

trait TController{
  def dispatch():Unit
}

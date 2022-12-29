package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd: RDD[Int] = sc.makeRDD(List[Int]())
    val user = new User()
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )
    sc.stop()
  }

    class User extends Serializable {
      var age: Int = 30
    }
//  case class User() {
//    var age: Int = 30
//  }

}



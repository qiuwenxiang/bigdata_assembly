package com.kylin.assembly.spark.streaming.demo

import org.apache.spark.sql.SparkSession

/**
  * 各种算子学习
  */
object ActionsLearning {

  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder
      .master("local")
      .appName("Word Count")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    var sc = spark.sparkContext
    val rddlist=sc.makeRDD(List(1,2,3,4,5))
    // map 算子  ,针对于每一个元素做操作，返回新集合
    val rl=rddlist.map(x=>x*x)
    println(rl.collect.mkString(","))

    // 自定义算子
    spark.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)

    


  }
}

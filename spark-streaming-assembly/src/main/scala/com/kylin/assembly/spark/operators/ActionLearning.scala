package com.kylin.assembly.spark.operators

import org.apache.spark.sql.SparkSession

/**
  * action 类操作算子
  */
object ActionLearning {

  val spark =SparkSession.builder
    .master("local")
    .appName("Word Count")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  var sc = spark.sparkContext


  /**
    * 循环
    */
  def foreachOper()={
    val c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
    c.foreach(x => println(x + "s are yummy"))
  }

  /**
    * 保存 ，windos不支持
    */
  def saveAsTextFileOper()={
    val a = sc.parallelize(1 to 10000, 3)
    a.saveAsTextFile("mydata_a")
  }

  /**
    * 保存 ，windos不支持
    */
  def saveAsObjectFileOper()={
    val a = sc.parallelize(1 to 10000, 3)
    a.saveAsObjectFile("mydata_a")
  }
  /**
    * 保存 ，windos不支持
    */
  def collectAsMapOper()={
    val a = sc.parallelize(1 to 10000, 3)
    val b = a.zip(a)
    b.collectAsMap()
  }

  def topOper()={
    val c = sc.parallelize(Array(6, 9, 4, 7, 5, 8), 2)
    c.top(2)
  }

  def foldOper()={
    val a = sc.parallelize(List(1,2,3), 3)
    a.fold(0)(_ + _)
  }
}

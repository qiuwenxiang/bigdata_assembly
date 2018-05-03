package com.kylin.assembly.spark.operators

import org.apache.spark.sql.SparkSession

/**
  * VALUE 型算子 各种算子学习
  */
object OperatorsLearning {

  val spark =SparkSession.builder
    .master("local")
    .appName("Word Count")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  var sc = spark.sparkContext

  /**
    * map算子 1对1 转换
    */
  def mapOper() = {
    // map 算子  ,针对于每一个元素做操作，返回新集合
    val rddlist=sc.makeRDD(List(1,2,3,4,5))
    // map 算子  ,针对于每一个元素做操作，返回新集合
    val rl=rddlist.map(x=>x*x)
    println(rl.collect.mkString(","))
  }

  /**
    * filter 算子，过滤出true的
    */
  def filterOper() = {
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 1)
    val evenNumberRDD = numberRDD.filter { number => number % 2 == 0 }
    evenNumberRDD.foreach { num => println(num) }
  }

  /**
    * flatMap算子,  先拆分为数组  再拼接为一个数组
    */
  def filterMapOper() = {
    val lines = Array("hello you","hello me","hello world")
    val linesRDD = sc.parallelize(lines, 1)
    val wordsRDD = linesRDD.flatMap { line => line.split(" ") }
    wordsRDD.foreach { word => println(word) }
  }


  /**
    * glom函数将每个分区形成一个数组
    */
  def glomOper() = {
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 2)
    val evenNumberRDD = numberRDD.glom()
    evenNumberRDD.foreach { num => num.foreach(x => println(x)) }
  }



  /**
    * union ++
    */
  def unionOper() = {
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD1 = sc.parallelize(numbers, 2)
    val numberRDD2 = sc.parallelize(numbers, 3)
    val finalRdd = numberRDD1++numberRDD2
    finalRdd.foreach(x => println(x))
  }

  /**
    * 传入函数 生成key
    * @return
    */
  def groupByOper() = {
    val a = sc.parallelize(1 to 9, 3)
    val result =a.groupBy(x => x)
    result.foreach(x => println(x))
  }

  /**
    * 去重
    */
  def distinctOper() = {
    val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    val result = c.distinct.collect
    result.foreach(x => println(x))
  }

  /**
    * 去掉重复的项
    * @return
    */
  def subtractOper() = {
    val a = sc.parallelize(1 to 9, 1)
    val b = sc.parallelize(1 to 3, 1)
    val c = a.subtract(b)
    c.collect
    c.foreach(x => println(x))
  }

  /**
    * 根据fraction指定的比例，对数据进行采样
    */
  def sampleOper() = {
    val a = sc.parallelize(1 to 10000, 3)
    var result = a.sample(false, 0.1, 0)
  }


  def main(args: Array[String]): Unit = {


    mapOper()

    filterOper()

    glomOper()

    filterMapOper()

    groupByOper()

    unionOper()

    distinctOper()

    subtractOper()

    sampleOper()



    // 自定义算子
   // scoresRDD.mapPartitions()


    spark.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)

  }
}

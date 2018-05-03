package com.kylin.assembly.spark.operators

import org.apache.spark.sql.SparkSession

/**
  * K-V 类型的 transformation 各种算子
  */
object KVOperatorsLearning {

  val spark =SparkSession.builder
    .master("local")
    .appName("Word Count")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  var sc = spark.sparkContext


  /**
    * groupByKey算子，参数是key-value型，按key分组
    */
  def groupByKeyOper() = {
    val scores = Array(("class1",80),("class2",90),("class1",65),("class2",85))
    val scoresRDD = sc.parallelize(scores, 1)
    val groupedscores = scoresRDD.groupByKey
    groupedscores.foreach(score => { println("class:"+score._1); score._2.foreach { s => println(s) };println("===============")})
  }

  /**
    * reduceByKey算子 ,参数是key-value型，按key分组 求和，也可做字符串拼接
    */
  def reduceByKeyOper() = {
    val scores = Array(("class1",80),("class2",90),("class1",65),("class2",85))
    val scoresRDD = sc.parallelize(scores, 1)
    val totalScores = scoresRDD.reduceByKey(_+_)
    totalScores.foreach(classScore => println(classScore._1+":"+classScore._2))
  }

  /**
    * sortByKey 算子 ，以key排序
    */
  def sortByKeyOper() = {
    val scores = Array(("class1",80),("class2",90),("class1",65),("class2",85))
    val scoresRDD = sc.parallelize(scores, 1)
    val sortedScores = scoresRDD.sortByKey()
    sortedScores.foreach(score => println(score._1+" : "+score._2))
  }



  /**
    * map的高效率版，对每个分区执行map
    * @return
    */
  def mapPartitionsOper() = {
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 2)

    val evenNumberRDD = numberRDD.mapPartitions((x) =>{
      var result = List[Int]()
      var i = 0
        while (x.hasNext){
        i+=x.next()
      }
      result.::(i).iterator
    })
    evenNumberRDD.foreach(x => println(x))
  }

  /**
    * mapPartitionsOper中添加分区数
    */
  def mapPartitionsWithIndexOper() = {
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 2)

    val evenNumberRDD = numberRDD.mapPartitionsWithIndex((index,x) =>{
      var result = List[Int]()
      var i = 0
      while (x.hasNext){
        i+=x.next()
      }
      result.::(index + "|" + i).iterator
    })
    evenNumberRDD.foreach(x => println(x))
  }

  /**
    * 改变key-value型对象的value值
    */
  def mapValuesOper() = {
    val textRDD = sc.parallelize(List((1, 3), (3, 5), (3, 7)))
    val mappedRDD = textRDD.mapValues(value => {value*value})
    mappedRDD.collect.foreach(println)
  }

  /**
    * 根据key统计数量
    */
  def countByKeyOper() = {
    val textRDD = sc.parallelize(List((1, 3), (3, 5), (3, 7), (3, 8), (3, 9)))
    val countRDD = textRDD.countByKey()
    countRDD.foreach(entity => println(entity._1+":"+entity._2))
  }

  /**
    * 两个rdd根据key相同组合
    */
  def combineByKeyOper() = {
    val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
    val c = b.zip(a)
    //在这个combineByKey中，可以看到首先每次遇到第一个值，就将其变为一个加入到一个List中去。
    //第二个函数指的是在key相同的情况下，当每次遇到新的value值，就把这个值添加到这个list中去。
    //最后是一个merge函数，表示将key相同的两个list进行合并。
    val d = c.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
    val result = d.collect
    result.foreach(x => println(x._1) )
  }

  def main(args: Array[String]): Unit = {


    mapPartitionsOper()

    mapPartitionsWithIndexOper()

    groupByKeyOper()

    reduceByKeyOper()

    sortByKeyOper()

    mapValuesOper()

    countByKeyOper()

    combineByKeyOper()


    // 自定义算子
   // scoresRDD.mapPartitions()


    spark.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)

  }
}

package com.kylin.assembly.spark.sql
import java.io.File

import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args : Array[String]) : Unit = {
    val warehouseLocation = new File("/spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits
    import spark.sql
    spark.sql("select sex,count(1) from default.t_user group by sex")

    spark.stop()

  }
}

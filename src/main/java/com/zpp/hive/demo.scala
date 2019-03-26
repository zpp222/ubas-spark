package com.zpp.hive

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object demo{
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder
      .appName("spark sql on hive test1")
      .enableHiveSupport()
      .getOrCreate()
    try{
      spark.sql("use erh")
      val users:Dataset[Row] = spark.sql("select name,count(1) from userinfo group by name")
      users.show(10)
    }catch {
      case ex:Exception =>
        print(ex)
    }finally {
      spark.stop()
    }
  }
}
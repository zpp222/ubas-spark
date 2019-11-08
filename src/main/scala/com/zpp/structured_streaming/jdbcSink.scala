package com.zpp.structured_streaming

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

class jdbcSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver";
  var statement: Statement = _
  var connection: Connection = _

  //创建连接
  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver);
    connection = DriverManager.getConnection(url, user, pwd);
    this.statement = connection.createStatement();
    true;
  }

  //执行sql
  override def process(kafkaData: Row): Unit = {
    statement.executeUpdate("insert into kafka_sink values('" + kafkaData.getAs("key") + "','" + kafkaData.getAs("value") + "','" + kafkaData.getAs("topic") + "','" + kafkaData.getAs("timestamp") + "')")
  }

  //关闭资源
  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}
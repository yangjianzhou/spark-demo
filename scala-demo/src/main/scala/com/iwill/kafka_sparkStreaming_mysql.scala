package com.iwill

import java.sql.{Connection, DriverManager, Statement}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object kafka_sparkStreaming_mysql {

  def myFun(records: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var statement: Statement = null

    try {
      val url = Constants.url
      val userName: String = Constants.userName
      val password: String = Constants.password

      conn = DriverManager.getConnection(url, userName, password)

      records.foreach(record => {
        val name = record._1.replaceAll("[\\[\\]]", "")
        val count = record._2
        print(name + "@" + count + "*********************")
        val sql = "select 1 from newscount where name = '" + name +"'"
        print(sql)
        val updateSql = "update newscount set count = count + " + count + " where name = '" + name + "'"
        print(updateSql)
        val insertSql = "insert into newscount(name,count) values('" + name + "'," + count + ")"
        print(insertSql)
        statement = conn.createStatement()
        var resultSet = statement.executeQuery(sql)
        if (resultSet.next()) {
          print("**************update**************")
          statement.executeUpdate(updateSql)
        } else {
          print("***************insert***************")
          statement.execute(insertSql)
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (statement != null) {
        statement.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def myFun2(records: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var statement: Statement = null

    try {
      val url = Constants.url
      val userName: String = Constants.userName
      val password: String = Constants.password

      conn = DriverManager.getConnection(url, userName, password)

      records.foreach(record => {
        val logtime = record._1
        val count = record._2
        print(logtime + "@" + count + "*********************")
        val sql = "select 1 from periodcount where logtime  = '" + logtime+"'"
        val updateSql = "update periodcount set count = count + " + count + " where logtime = '" + logtime+"'"
        val insertSql = "insert into periodcount(logtime,count) values('" + logtime + "'," + count + ")"
        statement = conn.createStatement()
        var resultSet = statement.executeQuery(sql)
        if (resultSet.next()) {
          print("**************update**************")
          statement.executeUpdate(updateSql)
        } else {
          print("***************insert***************")
          statement.execute(insertSql)
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (statement != null) {
        statement.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("sogoulogs").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Constants.kafkaServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Constants.groupId,
      "auto.offset.reset" -> Constants.offset,
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array(Constants.topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record => record.value())
    val filter = lines.map(_.split(",")).filter(_.length == 6)
    val newsCounts = filter.map(x => (x(2), 1)).reduceByKey(_ + _)
    newsCounts.foreachRDD(rdd => {
      print("--------------------------------------------")
      //分区并行执行
      rdd.foreachPartition(myFun)

    })
    newsCounts.print()
    //统计所有时段新闻浏览量
    val periodCounts = filter.map(x => (x(0), 1)).reduceByKey(_ + _)
    periodCounts.print()

    periodCounts.foreachRDD(rdd => {
      rdd.foreachPartition(myFun2)
    })

    ssc.start()
    ssc.awaitTermination()
  }


}

package com.iwill

import org.apache.spark.sql.SparkSession

object ScalaTest {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.appName("wordOperate").config("spark.master", "local").getOrCreate()
    val sc = sparkSession.sparkContext
/*    sc.parallelize()
    sc.textFile()*/

  }

}


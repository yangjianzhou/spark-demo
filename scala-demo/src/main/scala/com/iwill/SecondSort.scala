package com.iwill

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("spark-sort")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile("/Users/jiyang12/Github/spark-demo/scala-demo/src/main/resources/secondSort.txt")
    val pair:RDD[(SecondSortKey ,String)] = lines.map(line => (
      new SecondSortKey(line.split(" ")(0).toInt ,line.split(" ")(1).toInt),line
    )
    )
    val pairSort:RDD[(SecondSortKey ,String)] = pair.sortByKey()
    val result:RDD[String] = pairSort.map(line => line._2)
    result.foreach(line => println(line))
  }

}

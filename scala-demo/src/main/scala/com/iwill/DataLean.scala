package com.iwill

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DataLean {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DataLean")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val linesRDD = sc.textFile("/Users/jiyang12/Github/spark-demo/scala-demo/src/main/resources/dataLean.txt")
    linesRDD.count()
    linesRDD
      .flatMap(_.split(" "))
      .map((_, 1))
      .map(t => {
        val word = t._1
        val random = Random.nextInt(100)
        (random + "_" + word, 1)
      })
      .reduceByKey(_ + _)
      .map(t => {
        val word = t._1
        val count = t._2
        val realWord = word.split("_")(1)
        (realWord, count)
      })
      .reduceByKey(_ + _)
      .foreach(t => {
        println( t._1 + " : " + t._2)
      })
  }

}

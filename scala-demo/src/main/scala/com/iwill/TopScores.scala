package com.iwill

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopScores {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("top scores")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val linesRDD: RDD[String] = sc.textFile("/Users/jiyang12/Github/spark-demo/scala-demo/src/main/resources/topScores")
    val tupleRDD: RDD[(String, Int)] = linesRDD.map(line => {
      val name = line.split(",")(0)
      val score = line.split(",")(1)
      (name, score.toInt)
    })

    val top3 = tupleRDD.groupByKey().map(groupedData => {
      val name: String = groupedData._1
      val top3Scores: List[Int] = groupedData._2.toList.sortWith(_ > _).take(3)
      (name, top3Scores)
    })

    top3.foreach(tuple => {
      println("name : " + tuple._1)
      val tupleValue = tuple._2.iterator
      while (tupleValue.hasNext) {
        val value = tupleValue.next()
        println("score : " + value)
      }
      println("*********************")
    })
  }

}

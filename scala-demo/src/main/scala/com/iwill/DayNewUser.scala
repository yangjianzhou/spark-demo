package com.iwill

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DayNewUser {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("dayNewUser")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val tupleRDD:RDD[(String,String)] = sc.parallelize(
      Array(
        ("2020-01-01","user1"),
        ("2020-01-01","user2"),
        ("2020-01-01","user3"),
        ("2020-01-02","user1"),
        ("2020-01-02","user1"),
        ("2020-01-02","user4"),
        ("2020-01-03","user2"),
        ("2020-01-03","user5"),
        ("2020-01-03","user6"),
        ("2020-01-04","user8"),
        ("2020-01-04","user7"),
      )
    )

    val tupleRDD2:RDD[(String,String)] = tupleRDD.map(
      line => (line._2, line._1)
    )

    val groupedRDD:RDD[(String,Iterable[String])] = tupleRDD2.groupByKey()

    val dateRDD:RDD[(String,Int)] = groupedRDD.map(
      line => (line._2.min,1)
    )

    val resultMap:collection.Map[String,Long] = dateRDD.countByKey()

    resultMap.foreach(println)
  }

}

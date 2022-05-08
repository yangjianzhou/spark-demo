package com.iwill

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteHbase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkWriteHbase")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val initRDD =sc.makeRDD(
      Array(
        "003,王五,山东,23",
        "004,赵六,湖北,20",
      )
    )

    initRDD.foreachPartition(partition => {
      val hbaseConf = HBaseConfiguration.create()

      hbaseConf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3")
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val tableName = TableName.valueOf("student")

      val table = conn.getTable(tableName)

      partition.foreach(line => {
        val arr = line.split(",")
        val rowKey = arr(0)
        val name = arr(1)
        val address = arr(2)
        val age = arr(3)

        val put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(rowKey))

        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes("info"),org.apache.hadoop.hbase.util.Bytes.toBytes("name"),org.apache.hadoop.hbase.util.Bytes.toBytes(name))
        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes("info"),org.apache.hadoop.hbase.util.Bytes.toBytes("address"),org.apache.hadoop.hbase.util.Bytes.toBytes(address))
        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes("info"),org.apache.hadoop.hbase.util.Bytes.toBytes("age"),org.apache.hadoop.hbase.util.Bytes.toBytes(age))

        table.put(put)
      })
    })
  }
}

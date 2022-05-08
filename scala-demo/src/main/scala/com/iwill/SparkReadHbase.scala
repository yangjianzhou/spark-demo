package com.iwill

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkReadHbase {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkReadHbase")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE,"student")

    val hbaseRDD: RDD[(ImmutableBytesWritable ,Result)] = sc.newAPIHadoopRDD(
      hbaseConf ,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]
    )

    hbaseRDD.foreach{
      case (_, result: Result) =>
        val key = org.apache.hadoop.hbase.util.Bytes.toString(result.getRow)
        val name = org.apache.hadoop.hbase.util.Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
        val address = org.apache.hadoop.hbase.util.Bytes.toString(result.getValue("info".getBytes,"address".getBytes))
        val age = org.apache.hadoop.hbase.util.Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
        println("行键："+ key +"\t姓名: "+ name +"\t地址: "+ address +"\t 年龄: "+ age)
    }
  }
}

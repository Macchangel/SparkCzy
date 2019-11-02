package com.daslab.czy.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object HBaseReadTest {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val sc = new SparkContext(new SparkConf().setAppName("HBaseReadTest").setMaster("local"))

    val tableName = "test"
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    val rdd = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()

    rdd.foreach({case(_,result) =>
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("origin_data".getBytes,"text".getBytes))
      println("key: " + key + "  value: " + value)
    })
  }
}

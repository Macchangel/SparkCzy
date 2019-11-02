package com.daslab.czy.HBase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HBaseRead {
  def readFromHBase(sc: SparkContext, columnName: String, colunmnFamily: String, tableName: String) ={
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    val rdd = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()

    val outputRDD = rdd.map({case(_,result) =>
      val key = Bytes.toString(result.getRow)
      val value = SerializeUtils.deSerialize(result.getValue(colunmnFamily.getBytes,columnName.getBytes))
      (key,value)
    })

    outputRDD
  }

  def main(args: Array[String]): Unit = {
    val columnName = "text"
    val columnFamily = "origin_data"
    val tableName = "test"

    val sparkConf = new SparkConf().setAppName("ttt").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val outputRDD = readFromHBase(sparkContext, columnName, columnFamily, tableName)
    outputRDD.foreach(s =>{
      println("key:" + s._1 + " value:" + s._2)
    })
  }
}

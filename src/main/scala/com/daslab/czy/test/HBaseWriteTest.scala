package com.daslab.czy.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object HBaseWriteTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseWriteTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tableName = "test"

    val conf = HBaseConfiguration.create()

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val indataRDD = sc.makeRDD(Array(3,4,5))

    indataRDD.foreach(println)

//    val rdd = indataRDD.map(_.split(',')).map(arr =>{
//      val put = new Put(Bytes.toBytes(arr(0)))
//      put.addColumn(Bytes.toBytes("origin_data"),Bytes.toBytes("text"),Bytes.toBytes(arr(1)))
//      (new ImmutableBytesWritable(), put)
//    }).saveAsHadoopDataset(jobConf)
//
//    sc.stop()

  }

}

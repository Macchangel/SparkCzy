package com.daslab.czy.HBase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import com.daslab.czy.HBase._

object HBaseWrite {


  def saveToHBase(inputRDD: RDD[(String, Object)], columnName: String, colunmnFamily: String, tableName: String): Unit ={


    val conf = HBaseConfiguration.create()

    val jobConf = HBaseUtils.getJobConf(conf, tableName)

    inputRDD.map(row =>{
      val put = new Put(Bytes.toBytes(row._1))
      row._2 match {
        case str: String =>
          put.addColumn(Bytes.toBytes(colunmnFamily), Bytes.toBytes(columnName), Bytes.toBytes(str))
        case obj: Object =>
          put.addColumn(Bytes.toBytes(colunmnFamily), Bytes.toBytes(columnName), SerializeUtils.serialize(obj))
      }

      (new ImmutableBytesWritable(), put)
    }).saveAsHadoopDataset(jobConf)

  }

  def saveToHBase1(inputDF: DataFrame, columnFamily: String, tableName: String): Unit = {
    val conf = HBaseConfiguration.create()

    val jobConf = HBaseUtils.getJobConf(conf, tableName)

    var columnsNames = inputDF.columns
    columnsNames = columnsNames.drop(1).sorted

    inputDF.rdd.map(row => {
      val put = new Put(Bytes.toBytes(row.getAs[String]("id")))
      var columnName: Array[Byte] = null
      var value: Array[Byte] = null
      for (i <- 0 until columnsNames.length) {
        columnName = columnsNames(i).getBytes()
        value = Bytes.toBytes(row.getAs[String](columnsNames(i)))
        put.addColumn(Bytes.toBytes(columnFamily), columnName, value)
      }
      (new ImmutableBytesWritable(), put)
    }).saveAsHadoopDataset(jobConf)

  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(sparkConf)



//    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
//    val df = spark.createDataFrame(Seq(
//      ("1","I heard about Spark and I love Spark"),
//      ("2","I wish Java could use case classes"),
//      ("3","Logistic regression models are neat")
//    )).toDF("id","sentence")
//
//    saveToHBase(df, "origin_data", "test")
  }
}

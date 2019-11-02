package com.daslab.czy.test

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveByHFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataPreprocessing").master("local").getOrCreate()

    val df = spark.createDataFrame(Seq(
      ("1","I heard about Spark and I love Spark"),
      ("2","I wish Java could use case classes"),
      ("3","Logistic regression models are neat")
    )).toDF("id","sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(df)

    val hdfsPath = "hdfs://localhost:9000/user/hadoop/HFiles/"
    val tableName = "test"
    val columnFamily = "origin_data"
    val save_path = hdfsPath + tableName

    saveASHFile(wordsData, columnFamily, save_path)




  }

  def saveASHFile(resultDataFrame: DataFrame, columnFamily: String, save_path: String): Unit ={
    val conf = HBaseConfiguration.create()
    lazy val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    var columnsName = resultDataFrame.columns
    columnsName = columnsName.drop(1).sorted

    val result1 = resultDataFrame.rdd.map(row =>{
      var kvlist: Seq[KeyValue] = List()
      var rowKey: Array[Byte] = null
      var cn: Array[Byte] = null
      var v: Array[Byte] = null
      var kv: KeyValue = null
      val cf: Array[Byte] = columnFamily.getBytes()
      rowKey = Bytes.toBytes(row.getAs[String]("id"))
      for (i <- 1 until columnsName.length){
        cn = columnsName(i).getBytes()
        v = Bytes.toBytes(row.getAs[String](columnsName(i)))
        kv = new KeyValue(rowKey, cf, cn, v)
        kvlist = kvlist :+ kv
      }
      (new ImmutableBytesWritable(rowKey), kvlist)
    })

    val result = result1.flatMapValues(s => s.iterator)

    delete_hdfsPath(save_path)

    result
      .sortBy(x => x._1, true)
      .saveAsNewAPIHadoopFile(save_path,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],job.getConfiguration)



  }

  def delete_hdfsPath(url: String): Unit ={
    val hdfs: FileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration)
    val path: Path = new Path(url)
    if(hdfs.exists(path)){
      val filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ)
      hdfs.delete(path, true)
    }
  }
}

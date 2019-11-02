package com.daslab.czy.dataPreprocessing
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import com.daslab.czy.HBase.HBaseWrite

object TFIDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataPreprocessing").master("local").getOrCreate()

    import spark.implicits._

    val df = spark.createDataFrame(Seq(
      (0,"I heard about Spark and I love Spark"),
      (0,"I wish Java could use case classes"),
      (1,"Logistic regression models are neat")
    )).toDF("label","sentence")

    val outputDF = tfIdf(df)
    outputDF.rdd.foreach(println)

  }

  def tfIdf(inputDF: DataFrame): DataFrame = {
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(inputDF)
    wordsData.show()

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featuredData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featuredData)

    val rescaledData = idfModel.transform(featuredData)

    rescaledData
  }


}

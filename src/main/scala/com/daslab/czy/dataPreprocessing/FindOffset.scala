package com.daslab.czy.dataPreprocessing

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
import util.control.Breaks._
import com.daslab.czy.model._
import com.daslab.czy.HBase._
import org.apache.spark.rdd.RDD



object FindOffset {

  /**
   * 计算文章中单词的偏移量
   *
   *
   */
  def findOffset: Unit ={
    val sparkConf = new SparkConf().setAppName("FindPositon").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val columnName = "text"
    val columnFamily = "origin_data"
    val tableName = "test"

    val rdd = HBaseRead.readFromHBase(sc, columnName, columnFamily, tableName)
    val resultRDD: RDD[(String, Object)] = rdd.map(s =>{
      val text = s._2.asInstanceOf[String]
      val words = "\\w*".r findAllIn text
//      for(word <- words){
//        println(word)
//      }
      var pos = 0
      var offsets: Map[String,Tuple] = Map()
      while(words.hasNext && pos <= text.length){
        breakable{
          val word = words.next
          if(word == "") break
          var char = text(pos)
          while (char != word(0)){
            pos += 1
            char = text(pos)
          }
          if(offsets.contains(word)){
            offsets(word).addSpan(new Span(pos, pos + word.length -1))
          }else{
            offsets += (word -> new Tuple(Seq(new Span(pos, pos + word.length -1))))
          }
          pos += word.length
        }
      }
      println(offsets)

      (s._1, offsets)
    })


    HBaseWrite.saveToHBase(resultRDD, "offsets3", columnFamily, tableName)
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FindPositon").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val columnName = "offsets2"
    val columnFamily = "origin_data"
    val tableName = "test"
    val rdd = HBaseRead.readFromHBase(sc, columnName, columnFamily, tableName)

    rdd.foreach(s =>{
      println(s._2.getClass)
    })
  }
}

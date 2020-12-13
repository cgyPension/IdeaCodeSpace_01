package com.atguigu.SparkCore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My app")

    val sparkContext = new SparkContext(conf)

    val result: RDD[(String, Int)] = sparkContext.textFile("MySpark/input/hello*")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    println(result.collect().mkString(","))


    sparkContext.stop()
  }
}

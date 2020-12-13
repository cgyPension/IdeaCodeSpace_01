package com.atguigu.SparkCore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountWindow {
  def main(args: Array[String]): Unit = {
    //取消Master，后期可以通过spark -submit --master 进行设置
    val conf = new SparkConf().setAppName("My app")

    val sparkContext = new SparkContext(conf)

    // 进行wc的编程，读取输入的文件
    val rdd: RDD[String] = sparkContext.textFile(args(0))


    val rdd1: RDD[String] = rdd.flatMap(line => line.split(" "))


    val rdd2: RDD[(String, Int)] = rdd1.map(word => (word, 1))


    val result: RDD[(String, Int)] = rdd2.reduceByKey((value1, value2) => value1 + value2)


    println(result.collect().mkString(","))


    sparkContext.stop()
  }
}

package com.atguigu.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

class SparkStreaming02_Queue{
  def main(args: Array[String]): Unit = {

    // 创建应用的环境，指定3秒为数据的采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 执行逻辑
    val queue = new mutable.Queue[RDD[String]]()
    val queueDS: InputDStream[String] = ssc.queueStream(queue)

    queueDS.print()
    ssc.start()

    // 模拟向队列中放入RDD
    for ( i <- 1 to 5 ) {
      val rdd = ssc.sparkContext.makeRDD(List(i.toString))
      queue.enqueue(rdd)
      Thread.sleep(1000)
    }


    // 等待采集器的结束
    ssc.awaitTermination()

  }


}

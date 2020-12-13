package com.atguigu.SparkStreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object SparkStreaming06_KafkaDirectMode {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")

    // 创建应用的环境，指定3秒为数据的采集周期
    val streamingContext = new StreamingContext(conf, Seconds(3))

    //准备map，其中保存消费者的参数  不记得，参考ConsumerConfig
    val kafkaParams: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "enable.auto.commit" -> "false",
      "auto.commit.interval.ms" -> "500",
      "group.id" -> "kafkass",
      "client.id" -> "s1",
      "auto.offset.reset" -> "earliest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 存放每条 ConsumerRecord   消费
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("spark_streaming"), kafkaParams))

    // String:  每条记录的值
    val ds1: DStream[String] = ds.map(record => {
      /* if (record.value().equals("d")){
         //模拟异常
         throw new RuntimeException("发生了异常！")
       }*/
      record.value()
    })

    ds1.print(1000)

    //启动应用，开始计算
    streamingContext.start()

    // 会阻塞当前main线程，一直到应用停止
    streamingContext.awaitTermination()

  }
}

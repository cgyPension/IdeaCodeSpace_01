package com.atguigu.SparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreaming07_KafkaDirectModeCK {
  def main(args: Array[String]): Unit = {

    def getMySC()={
    // TODO 创建应用的环境，指定3秒为数据的采集周期
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //设置DS 周期型存储的目录
    ssc.checkpoint("kafka")

    // TODO 使用SparkStreaming读取Kafka的数据

    // Kafka的配置信息
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
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](List("spark_streaming"), kafkaParams)
      )

    val valueDStream: DStream[String] = kafkaDStream.map(record => {
      /*if (record.value().equals("d")){
      //模拟异常
      throw new UnknownError("发生了异常！")
    }*/
      record.value()
    })

    valueDStream.print(1000)

      ssc
    }

    // 从ck目录根据之前的状态重建 应用
    val streamingContext: StreamingContext = StreamingContext.getActiveOrCreate("kafka", getMySC)

    //启动应用，开始计算
    streamingContext.start()

    // 会阻塞当前main线程，一直到应用停止
    streamingContext.awaitTermination()

  }
}

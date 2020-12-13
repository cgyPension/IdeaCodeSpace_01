package com.atguigu.SparkStreaming.req_2.app


import com.atguigu.SparkStreaming.req_2.beans.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by VULCAN on 2020/9/14
 */
class BaseApp {

  val streamingContext = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("My app"), Seconds(3))

  def runApp(op: => Unit)={
    //自定义的逻辑
    op

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  //对接Kafka，提供一个方法从Kafka读取数据，封装为DStream
  def getDStreamFromKafka():InputDStream[ConsumerRecord[String, String]]={
    //准备map，其中保存消费者的参数  不记得，参考ConsumerConfig
    val kafkaParams: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "1000",
      "group.id" -> "kafkaex1",
      "client.id" -> "s1",
      "auto.offset.reset" -> "earliest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 存放每条 ConsumerRecord   消费
    val ds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("myTest05223"), kafkaParams))

    ds
  }

  // 从ConsumerRecord中获取数据，封装为Bean
  def getAllBeans(ds:InputDStream[ConsumerRecord[String, String]]): DStream[AdsInfo] ={

    // 1600054291787,华北,北京,101,3
    val beans: DStream[AdsInfo] = ds.map(record => {
      val words: Array[String] = record.value().split(",")
      AdsInfo(
        words(0).toLong,
        words(1),
        words(2),
        words(3),
        words(4)
      )
    })

    beans

  }

}

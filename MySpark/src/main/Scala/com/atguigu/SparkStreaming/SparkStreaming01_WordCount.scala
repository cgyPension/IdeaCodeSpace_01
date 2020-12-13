package com.atguigu.SparkStreaming

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.streaming.Durations.seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.junit.Test

class SparkStreaming01_WordCount {

  // todo Spark环境
  // SparkStreaming使用核数最少是2个，单核只够监听的资源，不够处理的资源
   val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My wordcount")
  // 创建应用的环境，指定3秒为数据的采集周期
   val streamingContext: StreamingContext = new StreamingContext(conf, seconds(3))

  @Test
  def WordCount1() : Unit ={
    /**
     *        连接 hadoop103 3333端口，读取端口发送的数据，进行wordcount
     *            发送： 每个单词以空格间隔
     */

    // 计算逻辑  创建编程模型DStream    收到的是文本数据，以UTF-8编码，以\n分割每行
    val ds: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop103", 3333)

    // ds中的方法称为 高度抽象原语
    val result: DStream[(String, Int)] = ds.flatMap(line => line.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    result.print(1000)

    //启动应用，开始输出
    streamingContext.start()

    //Thread.sleep(2000)
    // TODO 关闭
    // Driver程序执行streaming处理过程中不能结束
    // 采集器在正常情况下启动后不应该停止，除非特殊的情况.
    //streamingContext.stop()

    // 会阻塞当前main线程，一直到应用停止
    streamingContext.awaitTermination()
  }

}

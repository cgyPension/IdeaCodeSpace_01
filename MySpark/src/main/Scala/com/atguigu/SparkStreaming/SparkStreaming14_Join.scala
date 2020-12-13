package com.atguigu.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

class SparkStreaming14_Join {
  def main(args: Array[String]): Unit = {

    //Only one SparkContext should be running in this JVM (see SPARK-2243)
    //requirement failed: Some of the DStreams have different contexts  要求Join的两个流必须是从同一个StreamingContext创建
    val streamingContext1 = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("My app"), Seconds(3))
    val streamingContext2 = new StreamingContext(streamingContext1.sparkContext, Seconds(5))

    val ds1: ReceiverInputDStream[String] = streamingContext1.socketTextStream("hadoop103", 3333)
    val ds2: ReceiverInputDStream[String] = streamingContext2.socketTextStream("hadoop103", 4444)
    val ds3: DStream[(String, Int)] = ds1.transform(rdd => {
      rdd.map(x => (x, 1))
    })

    val ds4: DStream[(String, Int)] = ds2.transform(rdd => {
      rdd.map(x => (x, 1))
    })

    ds3.join(ds4).print(1000)

    streamingContext1.start()
    streamingContext2.start()

    // streamingContext1.stop()  not ok
    // 开一个线程，在分线程中，需要关闭应用时就关闭
    new Thread(){
      override def run(): Unit = {

        // 函数是来获取是否需要关闭的
        def ifShouldStop():Boolean={
          // 可以通过读取数据库中的某个值，读取HDFS上某个文件中的值
          true
        }

        //不断地获取是否需要关闭的信息
        while(!ifShouldStop ){

          //歇会,再去查询是否应该关闭
          Thread.sleep(5000)
        }

        //关闭应用    stopGracefully: 等最后一批收到的数据全部处理完关闭应用
        streamingContext1.stop(true,true)

      }
    }.start()

    streamingContext1.awaitTermination()
    streamingContext2.awaitTermination()

    // streamingContext1.stop()    not ok 被阻塞，执行不到
  }
}

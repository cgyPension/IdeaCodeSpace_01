package com.atguigu.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
class SparkStreaming12_Output {
  def main(args: Array[String]): Unit = {

    // TODO Spark环境
    // SparkStreaming使用核数最少是2个
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("cp")

    val ds = ssc.socketTextStream("localhost", 9999)
    // TODO 窗口
    val wordToOneDS = ds.map(num=>( "key", num.toInt ))


    // TODO reduceByKeyAndWindow方法一般用于重复数据的范围比较大的场合，这样可以优化效率
    // 有分为有状态和无状态
    val result = wordToOneDS.reduceByKeyAndWindow(
      (x, y) => {
        println(s"x = ${x}, y = ${y}")
        x + y
      },
      (a, b) => {
        println(s"a = ${a}, y = ${b}")
        a - b
      },
      Seconds(9)
    )

    //result.print()
    //result.saveAsObjectFiles()
    // transform : rdd => rdd
    // foreachRDD : rdd => Unit

    // rdd => map     => A => B
    // rdd => foreach => A => Unit
    // Code Driver(1)

    // todo  foreachRDD 本质是将每个采集周期收到的数据封装为1个RDD，交给Job处理，底层还是RDD
    result.foreachRDD(rdd=>{
      // Code Driver(M)
      rdd.foreach(
        data => {
          // Code Executor(N)
          println(data)
        }
      )
    })

    ssc.start()
    // 等待采集器的结束
    ssc.awaitTermination()
  }
}

package com.atguigu.SparkStreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
class SparkStreaming03_File {
  def main(args: Array[String]): Unit = {

    // TODO Spark环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO file1目录采集，识别文件的时间戳，所以没法识别旧文件，一般不会用这。会直接用Flume
    val dirDS: DStream[String] = ssc.textFileStream("input")
    dirDS.print()

    ssc.start()

    // 等待采集器的结束
    ssc.awaitTermination()
  }
}

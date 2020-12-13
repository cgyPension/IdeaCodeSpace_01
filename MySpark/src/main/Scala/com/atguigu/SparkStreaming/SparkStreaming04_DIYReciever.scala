package com.atguigu.SparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


class SparkStreaming04_DIYReciever {

  def main(args: Array[String]): Unit = {

    // TODO Spark环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 通过自定义的接收器，获取DStream
    val ds: ReceiverInputDStream[String] = ssc.receiverStream( new MyReceiver("localhost", 9999) )

    ds.print()

    ssc.start()
    // 等待采集器的结束
    ssc.awaitTermination()
  }

  // TODO 自定义数据采集器
  // 1. 继承Receiver，定义泛型。
  //    Receiver的构造方法有参数的，所以子类在继承时，应该传递这个参数
  //    StorageLevel 储存级别
  // 2. 重写方法
  //   onStart
  //.  onStop
  // 3. 参考stockStream、SocketReceiver
  class MyReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    /**
     *  收数据需要的组件：
     *             Socket： 连接端口
     *             BufferedReader : 一次读取输入流中的一行内容
     *
     *          在收数据之前，在onStart进行安装，在停止收数据时，在onStop清理
     */

    //提取属性
    private var socket: Socket = _
    private var reader: BufferedReader = _

    override def onStart(): Unit = {

      println(s"Connecting to $host:$port")
      try {
        socket = new Socket(host, port)
      } catch {
        case e: ConnectException =>
          restart(s"Error connecting to $host:$port", e)
          return
      }
      println(s"Connected to $host:$port")

      new Thread("Socket Receiver") {
        setDaemon(true)
        override def run() { receive() }
      }.start()
    }


    override def onStop(): Unit = {
      if (socket !=null){
        socket.close()
        socket=null
        println(s"Closed socket to $host:$port")
      }

      if (reader !=null){
        reader.close()
        reader=null
      }
    }

    def receive(): Unit = {
      reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream,
          "UTF-8"
        )
      )

      var s : String = null

      try {
        while (socket.isConnected && s != null) {
          s = reader.readLine()
          // TODO 将获取的数据保存到框架内部进行封装
          store(s)
          // 继续读取下一行
          s = reader.readLine()
        }
      } catch {
        case e:Exception =>
          println("Error receiving data", e)
          restart("重启继续收！")
      } finally {
        // 清理本次安装的组件
        onStop()
      }

    }

  }
}
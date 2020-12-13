package com.atguigu.SparkCore.exc

import com.atguigu.SparkCore.exc.UserVisitAction
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

abstract class BaseApp {

  val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("My app"))

  //当前需求结果的输出路径
  val pathName:String

  // op: Unit : 传入一个Unit类型的op(变量)  op:=>Unit : 传入一个没有参数列表的函数op
  //  控制抽象
  def runApp(op: =>Unit): Unit ={
    start()
    try {
      op
    } catch {
      case e:Exception => println(e.getMessage)
    } finally {
      sparkContext.stop()
    }

  }

  //读取输入目录中的数据，封装为bean
  def getAllData():RDD[UserVisitAction]={

    val rdd:RDD[String] = sparkContext.textFile("MySpark/input/user_visit_action.txt")
    val result:RDD[UserVisitAction] = rdd.map(line=>{
      val words: Array[String] = line.split("_")
      UserVisitAction(
        words(0),
        words(1).toLong,
        words(2),
        words(3).toLong,
        words(4),
        words(5),
        words(6).toLong,
        words(7).toLong,
        words(8),
        words(9),
        words(10),
        words(11),
        words(12).toLong
      )
    })
    result
  }

  def start(): Unit ={
    //删除output目录
    val filesystem: FileSystem = FileSystem.get(new Configuration)
    val path = new Path(pathName)
    if(filesystem.exists(path)){
      filesystem.delete(path,true)
    }
  }

}

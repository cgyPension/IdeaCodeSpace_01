package com.atguigu



import java.text.SimpleDateFormat

import scala.io.Source

/**
 * 求得每个用户一小时内最大的登录次数user_id,login_time
 */
object Test05 {

  def dataToLong(time:String): Long ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(time).getTime
  }

  def main(args: Array[String]): Unit = {
    val list=List(
      "a,2020-07-11 10:51:12",
      "a,2020-07-11 11:05:00",
      "a,2020-07-11 11:15:20",
      "a,2020-07-11 11:25:05",
      "a,2020-07-11 11:45:00",
      "a,2020-07-11 11:55:36",
      "a,2020-07-11 11:59:56",
      "a,2020-07-11 12:35:12",
      "a,2020-07-11 12:58:59",
      "b,2020-07-11 14:51:12",
      "b,2020-07-11 14:05:00",
      "b,2020-07-11 15:15:20",
      "b,2020-07-11 15:25:05",
      "b,2020-07-11 16:45:00",
      "b,2020-07-11 16:55:36",
      "b,2020-07-11 16:59:56",
      "b,2020-07-11 17:35:12",
      "b,2020-07-11 17:58:59"
    )



    //1、切割,时间转为long类型
    list.map(line=>{
      val arr = line.split(",")
      val name = arr.head
      val time = arr(1)
      (name,time)
    })
      .toList
      .groupBy(_._1)
      .map(line=>{
          val name = line._1
        val alldata = line._2

       // alldata.map({})
      })
    //

    //


    //
  }
}

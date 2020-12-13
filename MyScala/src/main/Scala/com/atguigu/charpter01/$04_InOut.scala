package com.atguigu.charpter01

import java.io.BufferedReader

import scala.io.{Source, StdIn}

object $04_InOut {
  def main(args: Array[String]): Unit = {
    //控制台输入数据
    val line = StdIn.readLine("请输入一行语句：")
    println(line)

    //读取文件
    //new BufferedReader()
    val bufferedSource = Source.fromFile("d:\\Program Files\\feiq\\Recv Files\\pmt.json","utf-8")
    bufferedSource.getLines().foreach(println(_))
  }
}

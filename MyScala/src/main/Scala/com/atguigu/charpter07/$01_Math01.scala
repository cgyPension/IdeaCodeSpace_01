package com.atguigu.charpter07

import scala.io.StdIn

/**
 * 模式匹配语法:
 *    变量 match {
 *       case 值 =>
 *          匹配后的逻辑
 *       case 值 =>
 *          匹配后的逻辑
 *    }
 */
object $01_Math01 {
  def main(args: Array[String]): Unit = {
  val x = StdIn.readLine()

    x match{
      case "spark"=>
        println("输入的是spark")
      case "hadoop"=>
        println("输入的是hadoop")
      case "flume" =>
        println("===========")
        println("输入的是flume")

      //case x =>
        //println(s"输入的是${x}")
        //如果变量不需要在右边使用，则可以用_代替
      case _=>
        println(s"输入的是其他字符串")
    }


  }
}

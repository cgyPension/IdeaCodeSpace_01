package com.atguigu.charpter07

import scala.io.StdIn

/**
 * 守卫
 * 模式匹配语法:
 *    变量 match {
 *       case 值 if 布尔表达式 =>
 *          匹配后的逻辑
 *       case 值  if 布尔表达式  =>
 *          匹配后的逻辑
 *    }
 *
 * 模式匹配有返回值
 */
object $02_Math_If {
  def main(args: Array[String]): Unit = {

    val line = StdIn.readLine()
    val result = line match{
      case x if x.contains("hello") =>{
        println(s"${x} 包含hello")
        10
      }
      case x if x.contains("spark") =>{
        println(s"${x} 包含spark")
        20
      }
      case x =>
        println("其他字符串")
        30
    }

    println(result)
  }
}

































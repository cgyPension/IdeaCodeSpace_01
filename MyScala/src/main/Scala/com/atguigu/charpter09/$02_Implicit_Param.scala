package com.atguigu.charpter09

/**
 * 隐式转换参数:
 *   语法: implicit val 变量:类型 = 值
 * 隐式转换参数要想使用必须在定义方法的时候表明哪个参数是要使用隐式转换参数
 * 定义方法的时候，隐式转换的参数单独放在一个参数列表中
 */
object $02_Implicit_Param {

  //定义隐式转换参数
  implicit val x:Double = 10
  def main(args: Array[String]): Unit = {

    println(m1(10)(30))

  }

  def m1(x:Int)(implicit y:Double) = {
    x+y
  }
}

package com.atguigu.charpter03

/**
 * 块表达式：
 *   1、定义: 由{}包裹的代码块称之为块表达式
 *   2、块表达式有返回值，返回值就是{}中最后一行表达式的结果值
 *
 * for循环、while循环是没有返回值的
 */
object $01_Block {

  def main(args: Array[String]): Unit = {

    val block: Unit = {
      val name = "zhangsan"
      val age = 20
      println(s"name=${name} age=${age}")
    }

    val block2 = {
      val a = 20
      a*20
    }


    //"hello"

    }


}

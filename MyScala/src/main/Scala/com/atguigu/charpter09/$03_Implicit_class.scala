package com.atguigu.charpter09

import java.io.File

import scala.io.Source
/**
 * 隐式转换类: 将一个类转成另一个类型
 * 语法:
 *   implicit class 类名(val 属性名:待转换类型) {
 *     .....
 *   }
 *
 * 后续当对象使用了不属于自身方法的时候就可以自动的转成隐式类的类型
 * 限制:
 *   1、隐式类必须定义在object或者class中，也就是说隐式类不能作为最顶端
 */

class BB{
  implicit class RichFile(val f:File){
    def readLines() = {
      Source.fromFile(f).getLines()
    }
    def add() = { 10+10 }
  }
}

object $03_Implicit_class {

  def main(args: Array[String]): Unit = {
    val file = new File("d:/data.txt")
    val bb = new BB
    //import spark.implicits._
    import bb.RichFile
    file.readLines()
    file.add()
  }
}

package com.atguigu.charpter07

import scala.io.StdIn
import scala.util.Random

object $03_Math_Value {
  def main(args: Array[String]): Unit = {
    val list = List[Any]("hello",10,false,2.2,100)

    val value = list(Random.nextInt(list.length))
    println(value)

/*    value match{
      case x:String => println(s"字符串${x}")
      case _:Double => println("double")
      case _:Boolean => println("boolean")
      case _:Int => println("int")
      case _ => println("其他类型")
    }*/

    //匹配的时候，如果匹配条件中是通过变量匹配(迷，和大小写无关，忽略这个匹配)
    //  1、如果变量名首字母是小写，则普通变量，后续值会传递给变量
    //  2、如果变量名首字母是大写，则匹配外部的常量，相当于就是匹配一个具体的值
    val Abc = "hello spark"
    val line = StdIn.readLine()
    line match {
      case Abc  if Abc.contains("hello") =>{
        println("=== " +Abc)
      }
      case _ => println("****")
    }

    //val aaa:Int = 10
  /*  aaa match{
      case x:String => println("字符串")
    }*/
  }
}

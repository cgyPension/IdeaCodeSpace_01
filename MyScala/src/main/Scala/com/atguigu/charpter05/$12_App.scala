package com.atguigu.charpter05

object $12_App {

  //DataFrame
  //type DataFrame = DataSet[Row]

  type s = String

  //枚举类
  object Color extends Enumeration{
    val RED = Value(1,"rea")
    val YELLOW = Value(2,"yellow")
    val BLUE = Value(3,"blue")
  }

  def main(args: Array[String]): Unit = {
    println("================")

    val name:s = "name"
    println(Color.RED)
  }
}

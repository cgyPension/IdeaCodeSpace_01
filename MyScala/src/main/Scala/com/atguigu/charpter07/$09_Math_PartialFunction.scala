package com.atguigu.charpter07

/**
 * 偏函数：[不使用match的模式匹配]
 *     val func:PartialFunction[IN,OUT] = {
 *        case .. => ..
 *     }
 */
object $09_Math_PartialFunction {
  def main(args: Array[String]): Unit = {
    val func:PartialFunction[Any,Int]={
      case x:String=>
        println("String")
        10
      case y:Int =>
        println("int")
        20
      case _=>
        println("other")
        30
    }

    val i:Int = func(100)
    println(i)

    val school = List(
      ("宝安中学",("小学一班",("1001","zhangsan",20))),
      ("宝安中学",("小学一班",("1002","lisi",22))),
      ("宝安中学",("小学一班",("1003","wangwu",23)))
    )

    //偏函数的应用场景
    val func1:PartialFunction[(String,(String,(String,String,Int))),String] = {
      case (schoolName,(className,(sid,sname,age))) => sname
    }

    println(school.map({
      case (schoolName,(className,(sid,sname,age))) => sname
    }))
  }
}

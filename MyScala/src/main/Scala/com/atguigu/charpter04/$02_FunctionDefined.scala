package com.atguigu.charpter04

/**
 * 函数定义:
 *     val 函数名 = (参数名:参数类型,...) => {函数体}
 *  在函数中不能使用return
 *  函数必须先定义在调用
 *
 *   函数就是对象
 *  方法与函数的区别:
 *     1、方法是随着类的加载而加载，函数只有定义在类中的时候才会随着类的加载而加载,如果定义在方法里面，只有调用方法的时候才会加载
 *     2、方法放在方法区中,函数放在堆中
 *
 *   方法转成函数:  方法名 _
 *   函数调用的时候必须带上()
 *
 *   函数的简化原则: 如果函数体中只有一行语句，则{}可以省略
 *
 *   函数没有重写、重载概念
 */
object $02_FunctionDefined {
  def main(args: Array[String]): Unit = {
    println(func1(2,3))

    val printMsg = (msg:String) => {
      if(msg==null) return null
      msg.format("hello")
    }
    //println(printMsg(null))

    println(func2(2,3))

    val func3 = add _  //方法转成函数
    println(func3(3,10))

    println(func5)
    val s = new String("s")
    s
  }

  val name = "zhangsan"
  val msg = name

  val func1 = (x:Int,y:Int) => x+y
  val func2 = func1

  def add(x:Int,y:Int) = x+y

  val func5 = () => println("+"*10)

  // 函数参数的个数
  // 最大个数为22，声明的时候可以超过22，但是将函数作为对象赋值给变量时会报错
  //val a : Func
  //        def test(
  //                i1 : Int,
  //                i2 : Int,
  //                i3: Int,
  //                i4 : Int,
  //                i5 : Int,
  //                i6 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                i1 : Int,
  //                ): Unit = {
  //
  //        }
}




































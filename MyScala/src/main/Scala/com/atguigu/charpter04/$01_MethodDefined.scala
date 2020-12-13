package com.atguigu.charpter04

/**
 * 方法定义语法:
 *     def 方法名(参数名:参数类型,...):返回值类型 = {方法体}
 *  方法的调用: 方法名(参数值,..)
 *
 *  方法的简化原则:
 *      1、如果方法的返回值是{}最后一行表达式的结果值，那么方法的返回值类型可以省略
 *           如果方法体中带有return关键字，则必须定义返回值类型
 *      2、如果方法体只有一行语句，{}可以省略
 *      3、如果方法不需要参数,()可以省略
 *           如果定义方法的时候没有加上(),在调用方法的时候也不能加上()
 *           如果定义方法的时候加上(),在调用方法的时候可以加上()，也可以省略()
 *      4、如果方法不需要返回值，=可以省略【=、{}不能同时省略】
 */
object $01_MethodDefined {
  def main(args: Array[String]): Unit = {

    val result = add(2,3)
    println(result)

    def add2(x:Int,y:Int):Int = {
      x*y
    }
    println(add2(3, 3))

    println(m2(10,20))

    printHello
    printHello1

    printMsg("zhangsan")
    println(helloPerson("hello %s","zhangsan"))
  }

  def add(x:Int,y:Int):Int ={
    x+y
  }

  //1、如果方法的返回值是{}最后一行表达式的结果值，那么方法的返回值可以省略
  def m1(x:Int) = {x*x}
  //2、如果方法体只有一行语句,{}可以省略
  def m2(x:Int,y:Int) = x+y
  //3、如果方法不需要参数，（）可以省略
  def printHello = println("hello")

  def printHello1() = println("hello")

  //4、如果方法不需要返回值，=可以省略
  def printMsg(msg:String) {
    println(msg)
  }

  def helloPerson(msg:String,arg:String):String = {
    if(msg==null) return null;
    msg.format(arg)
  }
}

package com.atguigu.charpter05

/**
 * Trait的基本语法: trait 特质名{ ... }
 *
 * scala中trait既可以定义抽象方法也可以定义具体方法
 * 既可以定义抽象字段也可以定义具体字段
 *
 * 子类继承trait：
 *     1、如果子类不需要继承class，第一个trait使用extends继承，后续的trait使用with关键字继承
 *     2、如果子类需要继承class,trait就只能通过with关键字继承
 *
 * scala是单继承多实现
 */
object $09_CreateTrait {

  abstract class Animal{
    //抽象字段
    val name:String
    //具体字段
    val age = 20
    //抽象方法
    def m1(x:Int,y:Int):Int

    //具体方法
    def m2(x:Int,y:Int) = x * y
  }


  trait Log{
    val xx = "AAA"
  }

  trait Log2{
    val yy = "YYY"
  }

  class Dog extends Animal with Log with Log2{
    override val name:String = "旺财"

    override def m1(x: Int, y: Int) = x + y
  }

  def main(args: Array[String]): Unit = {

    val dog = new Dog
    println(dog.name)
    println(dog.age)
    println(dog.m1(10, 20))
    println(dog.xx)
  }
}



























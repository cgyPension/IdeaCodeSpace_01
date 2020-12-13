package com.atguigu.charpter05

/**
 * 抽象类：
 *   1、抽象中既可以定义具体方法也可以定义抽象方法
 *   2、抽象类中如果定义了抽象方法，子类如果不是抽象类，那么就必须重写父类的抽象方法
 *   3、抽象类中既可以定义抽象属性[没有赋初始值的属性]也可以定义具体属性
 */
object $07_AbstarctClass {

  abstract class Animal{
    val name = "zhangsan"
    //抽象属性
    var age:Int

    def add(x:Int,y:Int) = x + y

    //抽象方法-没有方法体的方法
    def m1(a:Int,b:Int):Int
  }


  class Dog extends Animal{
    override var age:Int = 20

    //重写抽象方法
    def m1(a:Int,b:Int):Int = a + b
  }


  def main(args: Array[String]): Unit = {
    val dog = new Dog
    println(dog.age)
    println(dog.m1(2, 3))

    //匿名类
    val pig = new Animal {
      override var age: Int = 100

      override def m1(a: Int, b: Int): Int = a * b
    }

    println(pig.name)
    println(pig.m1(2, 3))
  }
}

























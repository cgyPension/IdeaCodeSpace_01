package com.atguigu.charpter05

/**
 * 对象混入trait:  只让某些对象拥有trait对应的属性与方法
 */
object $10_TraitObject {

  trait Log{
    def add(x:Int,y:Int) = x + y
  }

  class Dog{
    val name = "zhangsan"
  }

  def main(args: Array[String]): Unit = {
    val dog = new Dog with Log
    println(dog.add(3,9))
    val dog2 = new Dog
    //println(dog2.add())
  }
}

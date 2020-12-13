package com.atguigu.charpter10

/**
 * 上下界
 *     上界:  <:  泛型必须是指定类型或者其子类
 *     下界:  >: 泛型必须是执行类的父类或者其本身
 */
object $02_GenericUpperLower {

  class Parent

  class Person extends {
    def hello = println("hello")
  }

  class Student extends Person{
    override def hello: Unit = println("Student")
  }

  class ABC extends Person{
    override def hello: Unit = println("ABC")
  }

  def main(args: Array[String]): Unit = {

    m1(new ABC)
    val student:Any = new Student
    m2(student)
  }

  def m1[T<:Person](t:T) = {
    t.hello
  }

  def m2[T>:ABC](t:T) = {
    println(t)
  }
}




































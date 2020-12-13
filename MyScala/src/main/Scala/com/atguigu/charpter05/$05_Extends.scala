package com.atguigu.charpter05

import javax.xml.ws.BindingType

import scala.beans.BeanProperty

/**
 * 哪些不能被继承：
 *   1、final修饰class不能被继承
 *   2、private修饰的属性不可用被继承
 *
 * 父类var标识的属性不能被重写
 * 子类要想重写父类的方法必须通过override关键字标识
 * 子类可以重写父类val标识的非private的属性
 *
 * scala的多态是方法多态 属性也多态
 */
object $05_Extends {

  class Person{
    @BeanProperty
    val name = "zhangsan"

    var age = 20

    @BeanProperty
    val address = "beijing"

    def m1(x:Int,y:Int) = x + y
  }


  class Student extends Person{

    override val name = "lisi"

    override val address = "shenzhen"

    //override var age = 100
    override def m1(x:Int,y:Int) = mmmmmm(x,y)
  }


  def main(args: Array[String]): Unit = {

    val student = new Student
    //println(student.name)
    println(student.address)
    println(student.age)

    println("-------------------")

    val s = new Student
    println(s.getName)
    println(s.getAddress)

    val p:Person = new Student
    println(p.getName)
    println(p.getAddress)
  }
}



























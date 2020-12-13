package com.atguigu.charpter07

import scala.beans.BeanProperty

/**
 * 样例类:
 *   语法: case class 类名([val/var] 属性名:属性类型)
 * 样例对象:
 *   语法: case object object名称
 *
 * class Person(val name:String,val age:Int)
 *
 * object Person{
 *   def apply(name:String,age:Int) = new Person(name,age)
 * }
 *
 * Person("zhangsan",20)
 */
object $07_Math_Obect {

  case class Person(@BeanProperty name:String,@BeanProperty age:Int)

  trait SEX

  case object Man extends SEX
  case object Woman extends SEX

  def main(args: Array[String]): Unit = {
    val person = Person("zhangsan",20)
    println(person)
    println(person.getName)

    //person.age = 100
    println(person)


    m1(Man)
    person match{
      case Person(name,age) => println(s"name=${name} age=${age}")
    }

    val student = new Student("lisi",100)

    student match {
      //case x:Student=> println(s"x=${x.name}")
      case Student(name,age) => println(s"name:${name} age:${age}")
    }

  }

  class Student(val name:String,val age:Int)

  object Student{
    def unapply(arg: Student): Option[(String, Int)] = {
      if(arg==null)
        None
      else
        Some((arg.name,arg.age))
    }
  }

  def m1(sex:SEX) = {
    println(".............")
  }
}

































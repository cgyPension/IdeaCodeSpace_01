package com.atguigu.charpter05

import com.atguigu.charpter05.$01_CreateClass.Person

/**
 * scala的构造器分为两种: 主构造器、辅助构造器
 *   主构造器: 定义在类名后面通过([private val/var] 变量名:变量类型 = 默认值)表示
 *         val 定义的参数不能被重新赋值
 *         var 定义的参数可以被重新赋值
 *         val/var定义的参数直接可以当做class的属性来进行使用，可以在外部通过对象进行获取或者赋值
 *         不适用val/var定义的参数就只能在class内部使用
 *  辅助构造器:
 *      辅助构造器定义在class内部
 *      语法: def this(变量名：变量类型,...){
 *         //辅助构造器第一行必须调用主构造器或者其他的辅助构造器
 *      }
 */
object $03_ConstructMethod {

  class Person(val name: String = "zhangsan", var age: Int, address: String) {

    def this(age: Int) = {
      this(age = age, address = "shenzhen")
    }

    def this(address: String) = {
      this(age = 100, address = address)
    }

    def hello() = println(s"hello ${name} address = ${address}")
  }



  def main(args: Array[String]): Unit = {
    val person = new Person(age=20,address = "shenzhen")
    println(person.name)
    println(person.age)

    person.hello

    //println(person.address)

    val person1 = new Person(30)
    println(person1.name)
    println(person1.age)
    person1.hello()

    val person2 = new Person("bejing")
    println(person2.name)
    println(person2.age)
    person2.hello()
  }

}

































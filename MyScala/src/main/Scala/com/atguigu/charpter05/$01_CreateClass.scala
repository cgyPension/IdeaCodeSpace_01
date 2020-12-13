package com.atguigu.charpter05

/**
 * java 创建class
 * public class 类名{
 *
 * }
 *
 * scala中没有public关键字，默认就是相当于public
 * class 类名{
 *
 * }
 *scala中如果class中不需要定义属性与方法，{}可以省略: class 列名
 * scala中创建对象的时候，如果不需要传参数，()可以省略
 */
object $01_CreateClass {

  class Person

  def main(args: Array[String]): Unit = {
    val person = new Person
    println(person)
  }
}

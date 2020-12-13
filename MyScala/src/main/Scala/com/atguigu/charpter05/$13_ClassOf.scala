package com.atguigu.charpter05

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty
/**
 *  获取类的class： classOf[类名]
 *  获取对象的class: 对象.getClass
 */
object $13_ClassOf {

   case class Person( name:String, age:Int)

  def main(args: Array[String]): Unit = {

    val json = """{"name":"lisi","age":100}"""

    //java:
    //  1、根据类获取class。类名.class
    //  2、如果根据对象获取class。 对象.getClass

    val person = JSON.parseObject(json,classOf[Person])

    println(person.name)
    println(person.age)

    println(person.getClass)
    println(classOf[Person])
  }
}

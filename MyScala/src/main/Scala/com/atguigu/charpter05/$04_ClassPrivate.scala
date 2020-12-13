package com.atguigu.charpter05

import scala.beans.BeanProperty

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
/**
 * 封装: 属性私有，提供公有get/set方法
 * scala提供了@BeanProperty，会自动生成属性的get/set方法
 * @BeanProperty修饰的属性不能通过private修饰
 */
object $04_ClassPrivate {

  class Person{
    @BeanProperty
    val name = "zhangsan"

    @BeanProperty
    var age = 20
  }

  def main(args: Array[String]): Unit = {

    val person = new Person
    println(person.getName)
    person.setAge(100)
    println(person.getAge)

    //对象转json
    val json = JSON.toJSONString(person,null.asInstanceOf[Array[SerializeFilter]])
    println(json)
  }
}

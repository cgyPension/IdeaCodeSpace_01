package com.atguigu.charpter05

//java中定义属性与方法
//修饰符 类型 变量名 = 值
//scala中定义属性
//修饰符 val/var 变量名:变量类型 = 值
//成员属性
//成员变量用val定义之后，不可用被重新赋值
//scala中成员变量如果使用var定义，有默认的set方法[ 属性名= ]
//scala中var定义的变量可以通过_赋默认值[ null、0、false、0.0 ]


object $02_ClassFieldMethod {

  class Person {
    val name = "zhangsan"
    var age:Int = _

    //成员方法
    def add(x: Int, y: Int) = x + y
  }

  def main(args: Array[String]): Unit = {

    val person = new Person

    println(person.name)
    //
    //person.name = "lsii"

    println(person.age = 100)
    println(person.add(2,3))

  }
}

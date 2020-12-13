package com.atguigu.charpter05

/**
 * scala中可以实现多个trait,如果这多个trait都有同名方法。此时子类创建的对象在执行的时候就会报错
 * 解决方案:
 *     直接重写同名方法。如果在方法中调用父类的同名方法，实际上调用的是实现顺序中最后一个trait的同名方法
 *
 * 同名方法的执行顺序[继承的多个trait有一个共同的父trait/class]:按照继承顺序从右向左执行(有super调用父类时，没有则反之)
 * 实例化顺序: 按照继承/实现的顺序从左往右开始实例化
 */
object $11_TraitMethod {

  trait Log{
    println("log=============")
    def log(msg:String) = println(s"log:${msg}")
  }

  trait WarnLog extends Log{
    println("WarnLog============")
    override def log(msg: String) = {
      println(s"warn:${msg}")
      super.log(msg)
    }
  }

  trait ErrorLog extends Log{
    println("ErrorLog===================")
    override def log(msg: String) = {
      println(s"error:${msg}")
      super.log(msg)
    }
  }

  class Dog extends WarnLog with ErrorLog{
    println("Dog==================")
    override def log(msg: String) = {
      println(s"dog :${msg}")
      super.log("lisi")
    }
  }

  def main(args: Array[String]): Unit = {
    val dog = new Dog

    dog.log("hello")
  }
}




















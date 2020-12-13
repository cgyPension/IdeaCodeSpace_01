package com.atguigu.charpter10


/**
 * 泛型类
 *   语法: class 类名[泛型,泛型,..] (val xx:泛型类型,..) {
 *       def yy(aa:泛型类型) : 泛型类型 = {..}
 *   }
 */

class Person[T,A](val name:T,val age:A){
  def getName():T = this.name
  def getAget():A = this.age
}

object $01_GenericMethod {
  def main(args: Array[String]): Unit = {
    m1(Array[Int](1,2,3,4,5))
    val person = new Person[String,Int]("zhangsan",20)
    println(person.getName())
  }


  /**
   * 泛型方法:
   *   语法: def 方法名[泛型](x:泛型):泛型
   */
  def m1[T](x:Array[T]) = {
    println(x.toBuffer)
  }
}
































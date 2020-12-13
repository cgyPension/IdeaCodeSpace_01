package com.atguigu.charpter10

/**
 * 逆变: -T
 *   创建对象之后，对象的关系与泛型的关系相反
 * 协变: +T
 *   创建对象之后，对象的关系与泛型的关系保持一致
 * 非变: T
 *   创建对象之后，两个对象没有任何关系
 */
object $03_GenericNoChange {
  def main(args: Array[String]): Unit = {

    //非变
    //var abc = new ABC[Parent]
    //var abc2 = new ABC[Sub1]

    //abc = abc2

    //协变
/*    var abc = new ABC[Parent]
    var abc2 = new ABC[Sub1]
    abc = abc2*/

    //逆变
    var abc = new ABC[Parent]
    var abc2 = new ABC[Sub1]
    abc2 = abc
  }

  class Parent

  class Sub1 extends Parent

  //class ABC[T]
  //class ABC[+T]
  class ABC[-T]
}

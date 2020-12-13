package com.atguigu.charpter04

/**
 * 柯里化: 有多个参数列表的方法称之为柯里化
 */
object $09_KerroyFunction {
  def main(args: Array[String]): Unit = {
    println(m1(20,30))

    val func1:(Int,Int) => Int => Int = m3(2,3)

    val func2 = func1(4,5)

    val result = func2(10)
    println(result)

    println(m3(2,3)(4,5)(10))

    println(m2(2,3)(4,5)(10))

    val func4: (Int,Int) => (Int,Int) => Int => Int = m2 _

    val func5: (Int,Int) => (Int,Int) => Int => Int = m3 _
  }

  def m1(x: Int, y: Int) = x + y
  def m2(x:Int,y:Int)(z:Int,s:Int)(a:Int) = x+y+z+s+a

  /**
   * 演化过程
   *   方法/函数可以返回函数
   */
  def m3(x:Int,y:Int) = {
    val func = (z:Int,s:Int) => {
      val func1 = (a:Int) => {
        x+y+z+s+a
      }
      func1
    }
    func
  }
}

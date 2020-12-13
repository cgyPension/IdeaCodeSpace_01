package com.atguigu.charpter04

/**
 * 匿名函数: 没有函数名的函数
 * 匿名函数不能直接调用，只能作为值进行传递。
 */
object $08_NiMing {
  def main(args: Array[String]): Unit = {
    val func = (x: Int, y: Int) => x + y
    //(x: Int, y: Int) => x + y
    m1(10,20,(x:Int,y:Int) => x+y)
  }

  def m1(x:Int,y:Int,func:(Int,Int) => Int) ={
    func(x,y)
  }
}

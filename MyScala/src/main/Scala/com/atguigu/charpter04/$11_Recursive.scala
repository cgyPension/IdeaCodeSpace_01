package com.atguigu.charpter04

/**
 * 递归: 自己调用自己
 *   1、在方法体中要有退出条件
 *   2、方法必须定义返回值类型
 *
 */
object $11_Recursive {
  def main(args: Array[String]): Unit = {
    println(m1(5))
  }

  //5 * 4 * 3 * 2 * 1
  def m1(n:Int):Int = {

    if(n==1){
      1
    }else{
      n*m1(n-1)  // 5 * m1(4)[ 4 * m1(3)[ 3 * m1(2)[ 2* m1(1)[1]]]]
    }
  }
}

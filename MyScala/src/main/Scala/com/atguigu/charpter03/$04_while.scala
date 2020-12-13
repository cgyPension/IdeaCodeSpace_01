package com.atguigu.charpter03

/**
 * scala的while与do-while跟java用法一样
 * while与do-while的区别：
 *   while是先判断在循环
 *   do-while是先循环再判断
 */
object $04_while {
  def main(args: Array[String]): Unit = {

    var i = 11
    while (i<=10){
      println(i*i)
      i= i + 1
    }

    println("="*100)
    var j = 11
    do{
      println(s"j*j=${j*j}")
      j = j+1
    }while(j<=10)

  }
}

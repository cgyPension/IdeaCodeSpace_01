package com.atguigu.charpter03

/**
 *
 * java中if用法：
 *   1、单if
 *   2、if-else
 *   3、if-else if-..-else
 * scala
 *   1、单if
 *   2、if-else
 *   3、if-else if-..-else
 * scala中if else与java中不一样的地方在于,if-else有返回值。
 * if-else的返回值就是满足条件的一个分支的块表达式[{}最后一行表达式的结果值]的结果值
 */

object $02_if_else {
  def main(args: Array[String]): Unit = {
    val a = 20

    val b = if(a%2==0){
      10
    }else if(a==20){
      20
    }else{
      30
    }

    println(b)

    val c = 50
    val result = if(c%2==0){
      val d = 50
      if(d%2==0 && c%5==0){
        d+c
      }else{
        c
      }
    }else{
      150
    }

  }
}






























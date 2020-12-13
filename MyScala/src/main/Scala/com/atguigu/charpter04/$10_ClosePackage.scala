package com.atguigu.charpter04

import java.util
/**
 * 闭包: 闭包是函数，在函数体中使用了不属于函数本身变量
 *
 */
object $10_ClosePackage {
  def main(args: Array[String]): Unit = {

    val data = new util.HashMap[String,String]()
    data.put("scala","http://www.scala-lang.org")
    data.put("hadoop","http://hadoop.apache.org")
    data.put("spark","http://spark.apache.org")

    def map(arr:Array[String],func:String=>String) ={
      for(element<- arr) yield func(element)
    }

    val func = (x:String) => {
      data.get(x)
    }

    val arr = Array("scala","hadoop","spark")

    println(map(arr,func).toBuffer)
  }

  val y = 10
  val func = (x:Int) => {
    x+y
  }
}

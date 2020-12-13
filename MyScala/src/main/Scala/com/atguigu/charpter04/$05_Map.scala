package com.atguigu.charpter04

/**
 * #1、定义一个高阶函数，对传入的数组中的每个元素进行操作，将其变成单词的长度
 * Array("hello","word","hadoop","python")
 * =>
 * Array(5,4,6,6)
 */

object $05_Map {
  def main(args: Array[String]): Unit = {
    val arr = Array("hello","word","hadoop","python")
    map(arr,(x:String) => x.length)
    //省略函数类型
    map(arr,(x) => x.length)
    //函数只有一个参数，可以省略（）
    map(arr,x => x.length)
    //参数在函数体中只使用了一次，可以用_代替
    println(map(arr,_.length).toBuffer)
  }



  def map(arr:Array[String],func:String => Int) = {
      for(elecment<- arr) yield func(elecment)
  }


}




































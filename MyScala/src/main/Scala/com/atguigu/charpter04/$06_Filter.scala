package com.atguigu.charpter04

/**
 * #2、定义一个高阶函数，参数两个:Array,函数，通过传入的函数对Array进行过滤，保留符合我们要求的数据
 * Array(1,3,6,4,2,9)
 * =>
 * Array(6,4,2)
 **/
object $06_Filter {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,3,6,4,2,9)
    val func =
      //println(filter(arr,(x:Int) => x%2==0).toBuffer)
      //println(filter(arr,x => x%2==0).toBuffer)
      //println(filter(arr,x => x%2==0).toBuffer)
    println(filter(arr,_%2==0).toBuffer)
  }

  def filter(arr:Array[Int],func:Int => Boolean) = {
    for(element<- arr if(func(element))) yield{
      element
    }
  }
}




































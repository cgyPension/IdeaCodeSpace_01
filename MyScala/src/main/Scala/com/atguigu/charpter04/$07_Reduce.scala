package com.atguigu.charpter04

/**
 * #3、定义一个高阶函数，参数两个:Array,函数，根据函数的规则对数组进聚合
 * Array(1,3,6,4,2,9,8,10)
 * =>
 * 25
 **/
object $07_Reduce {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,3,6,4,2,9,8,10)

    //println(reduce(arr,(x:Int,y:Int) => x*y))
    //println(reduce(arr,(x,y) => x*y))
    println(reduce(arr,_*_))
  }

  def reduce(arr:Array[Int],func:(Int,Int) => Int) = {
    var agg = arr(0)

    for(i<- 1 until(arr.length)){
      println(s"agg=${agg} curr=${arr(i)}")
      agg = func(agg,arr(i))
    }
    agg
  }
}













































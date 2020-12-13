package com.atguigu.charpter06

import scala.collection.mutable.ArrayBuffer

/**
 * 创建可变数组: ArrayBuffer[元素类型](初始元素,...)
 *   单个+/-与两个+/-的区别: 单个+/-针对的是单个元素，两个+/-针对是一个集合的所有元素
 *   +与-的区别： +代表添加元素,-代表删除元素
 *   带=与不带=的区别: 带=修改的是集合本身,不带=创建一个新集合，原有集合不变
 *   :在前与:在后的区别:  :在前面代表添加元素到最后面，:在后代表添加元素到最前面。不带:是将元素添加到最后面
 * 添加数据
 * 删除
 * 查看
 * 修改数据
 * 可变数组转不可变数组: toArray
 */
object $02_MutableArray {
  def main(args: Array[String]): Unit = {

    val arr = ArrayBuffer[Int](2,6,4)
    println(arr)

    //1、查看指定角标的数据
    println(arr(0))

    //2、修改指定角标的值
    arr(0) = 100
    println(arr(0))
    //3、添加元素
    //生成新集合
    val arr2 = arr.+:(5)
    println(arr2)
    println(arr)

    val arr3 = arr.++(Array(1,2,3))
    println(arr3)
    println(arr)

    val arr4 = arr.++:(Array(10,20,30))
    println(arr4)
    println(arr)

    //修改集合本身
    val arr5 = arr.+=(10)
    println(arr)

    arr.+=:(20)
    //ArrayBuffer(20,100,6,4,10)
    println(arr)

    arr.++=(Array(40,50,60,60))
    println(arr)

    //删除元素
    val arr6 = arr.-(100)
    println(arr6)
    println(arr)

    val arr7 = arr.--(Array(10,20,30))
    println(arr7)

    arr.-=(100)
    println(arr)

    arr.--=(Array(60,70,80,90))
    println(arr)

    val arr8 = arr.toArray
    //创建多维数组
    val arr9 = Array.ofDim[Int](2,3)
    println(arr9(0).length)

  }
}

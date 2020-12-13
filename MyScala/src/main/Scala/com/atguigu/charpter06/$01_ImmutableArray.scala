package com.atguigu.charpter06

/**
 * 1、创建不可变数组：
 *     1、 new Array[元素类型](长度)
 *     2、 Array[元素类型](初始元素,..)
 * 2、增删改查
 *   1、获取元素:  数组(角标)
 *   2、修改元素的值:  数组(角标) = 值
 *   3、添加元素[实际上是生成一个新的数组，原有的数组没有变化]
 *     添加单个元素:  +:与:+的区别【+:将元素添加到最前面,:+将元素添加到最后面】
 *       val 新数组 = 数组.+:(元素)
 *       val 新数组 = 数组.:+(元素)
 *    添加集合的所有元素: ++ 、++: [++将一个数组的所有元素添加到最后面，++：将一个数组的所有元素添加到最前面]
 *       val 新数组 = 数组.++(数组2)
 *       val 新数组 = 数组.++:(数组2)
 *
 * 不可变数组转可变数组: toBuffer
 */
object $01_ImmutableArray {
  def main(args: Array[String]): Unit = {
    //1、new Array[元素类型](长度)
    val arr1 = new Array[Int](5)
    println(arr1.toBuffer)
    //2、Array[元素类型](初始元素,..)
    val arr2 = Array[Int](1,2,3,6,8)
    println(arr2.toBuffer)
    //3、获取元素 数组对象(角标)
    println(arr2(0))

    //4、修改元素
    arr2(0)=100
    println(arr2)

    //5、添加单个元素
    //对于不可变数组，添加元素的时候只生成一个新的数组，原来的数组没有改变
    val arr3 = arr2.+:(20)
    val arr4 = arr2.:+(30)
    println(arr2.toBuffer)
    println(arr3.toBuffer)
    println(arr4.toBuffer)
    //scala中==与eqauls含义一样，如果想判断两个对象是否同一个，需要通过eq方法来判断
    println(arr2.eq(arr3))

    //6、添加集合的所有元素
    val arr5 = arr2.++(arr1)
    println(arr5.toBuffer)
    println(arr2.toBuffer)
    val arr6 = arr2.++:(arr1)
    println(arr6.toBuffer)

  }
}

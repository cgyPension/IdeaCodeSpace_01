package com.atguigu.charpter06

/**
 * set的特性: 无序不重复
 */
object $05_ImmutableSet {
  def main(args: Array[String]): Unit = {

    val set = Set[Int](2,5,6)
    println(set)

    //是否包含元素，一般不用这种形式
    //println(set(2))

    val set1 = set.+(10)
    println(set1)

    val set3 = set.++(Array(10,100,200,300))
    println(set3)

    val set4 = set.++:(Array(10,100,200,300))
    println(set4)

    //删除
    val set5 = set.-(5)
    println(set5)

    val set6 = set.--(Array(2,5,6))
    println(set6)
  }
}




















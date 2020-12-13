package com.atguigu.charpter06

object $11_CollectionFunction_2 {
  def main(args: Array[String]): Unit = {

    val list = List[Int](10, 30, 20, 90, 100, 55, 100)

    //删除前多少个元素
    val list2 = list.drop(3)
    println(list2)

    //删除后多少个元素
    val list3 = list.dropRight(2)
    println(list3)

    //去重******
    val list4 = list.distinct
    println(list4)

    //获取第一个元素*****
    println(list.head)
    //获取最后一个元素*****
    println(list.last)
    //获取除开最后一个元素的所有元素
    val list5 = list.init
    println(list5)
    //是否为空*****
    println(list.isEmpty)
    //反转
    println(list.reverse)
    //划窗****
    val result = list.sliding(3, 2)
    for (x <- result) {
      println(x.toBuffer)
    }

    //获取子集合 form：开始角标 until：结束角标[不包含] ****
    val list6 = list.slice(2, 5)
    println(list6)

    //获取除开第一个元素的所有元素 *****
    val list7 = list.tail
    println(list7)

    //获取前多少个元素 ****
    val list8 = list.take(3)
    println(list8)

    //获取后多少个元素
    val list9 = list.takeRight(3)
    println(list9)

    //集合操作：
    //交集 去两个集合共同的部分
    //val list  = List[Int](10,30,20,90,100,55,100)
    val list10 = list.intersect(List(100,200,300))
    println(list10)

    //差集 -- A 差 B 保留的是A中有B中没有的元素
    val list11 = list.diff(List(10,20,100,200))
    println(list11)

    //冰并集
    val list12 = list.union(List(100,200,300))
    println(list12)

    //拉链
    val list13 = list.zip(List("zhangsan","lisi","wangwu","aa","bb","cc","dd"))
    println(list13)
    val list14 = list.zip(List("aa","bb"))
    println(list14)

    //反拉链
    val list15:(List[Int],List[String]) = list13.unzip
    println(list15)
  }
}





























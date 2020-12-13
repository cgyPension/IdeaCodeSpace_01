package com.atguigu.charpter06

object $03_ImmutableList {
  def main(args: Array[String]): Unit = {

    val list = List[Int](1,2,3,4)

    println(list(0))

    val list2 = list.updated(0,100)
    //list(0)=100
    println(list2)

    //添加单个元素
    val list3 = list.+:(10,50)
    println(list3)

    val list4 = list.:+(20)
    println(list4)

    //::是添加单个元素
    val list7 = list.::(30)
    println(list7)

    //添加一个集合的所有元素
    val list5 = list.++(List(10,20,30))
    println(list5)

    val list6 = list.++:(List(100,200))
    println(list6)

    //:::是添加一个集合的所有元素
    val list8 = list.:::(List(100,20,300))
    println(list8)
  }
}




















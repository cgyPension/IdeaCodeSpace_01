package com.atguigu.charpter06

import scala.collection.mutable.ListBuffer

object $04_MutableList {
  def main(args: Array[String]): Unit = {
    //创建可变List
    val list = ListBuffer[Int](10,20,30)
    println(list)

    //查看数据
    println(list(0))

    //修改数据
    list.update(0,40) //修改集合本身
    println(list)

    val list1 = list.updated(0,50)  //生成新集合,注意末尾的字母d
    println(list1)
    println(list)

    //添加数据
    val list2 = list.+:(40)
    println(list2)
    list.+=(50)
    println(list)
    list.+=:(60)
    println(list)

    val list3 = list.++(List(100,200))
    println(list3)
    val list4 = list.++:(List(300,400))
    println(list4)

    list.++=(List(500,600))
    println(list)

    list.++=:(List(700,800))
    println(list)
    //删除数据
    val list8 = list.-(40)
    println(list8)

    list.-=(800)
    println(list)

    val list9 = list.--(List(20,30,50,60))
    println(list9)

    list.--=(List(700,600,500,400))
    println(list)

  }
}










































package com.atguigu.charpter06

import scala.collection.mutable


object $06_MutableSet {
  def main(args: Array[String]): Unit = {

    val set = mutable.Set[Int](10,20,30)
    println(set)

    val set1 = set.+(40)
    println(set1)
    set.+=(50)
    println(set)

    val set3 = set.++(Array(10,20,30.40,50,60))
    println(set3)

    set.++=(Array(100,200))
    println(set)

    //删除
    val set5 = set.-(100)
    println(set5)
    set.-=(200)
    println(set)

    val set6 = set.--(List(20,30))
    println(set6)
    set.--=(List(100,50))
    println(set)
  }
}

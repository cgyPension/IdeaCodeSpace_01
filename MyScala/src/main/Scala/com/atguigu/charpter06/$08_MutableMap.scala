package com.atguigu.charpter06

import scala.collection.mutable

/**
 * 创建map:
 *   1、Map[K的类型,V的类型]( K -> V,K->V,..)
 *   2、Map[K的类型,V的类型]( (K,V),(K,V))
 */
object $08_MutableMap {
  def main(args: Array[String]): Unit = {

    val map = mutable.Map[String,Int]("zhangsan"->20,("lisi",30))
    println(map)

    //获取key对应的value
    println(map.getOrElse("zhangsan", ""))

    //修改value值[若果key存在，则修改，如果key不存则插入]
    map("zhangsan") = 40
    println(map)

    //添加
    val map1 = map.+(("wangwu",60))
    println(map1)
    map.+=(("tianqi",70))
    println(map)

    val map3 = map.++(Map[String,Int]("aa"->1,"bb"->2))
    println(map3)

    map3.++=(Map[String,Int]("cc"->10,"dd"->20))
    println(map3)

    map.put("K",100)
    println(map)

    //删除
    val map4 = map.-("aa")
    println(map4)
    map.-=("lisi")
    println(map)

    val map5 = map.--(List("zhangsan","tianqi"))
    println(map5)

    map.--=(List("zhangsan"))
    println(map)

    map.remove("K")
    println(map)
  }
}






















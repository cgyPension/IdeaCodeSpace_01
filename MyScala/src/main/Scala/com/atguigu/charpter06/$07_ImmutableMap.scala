package com.atguigu.charpter06

/**
 * 创建map:
 *   1、Map[K的类型,V的类型]( K -> V,K->V,..)
 *   2、Map[K的类型,V的类型]( (K,V),(K,V))
 */
object $07_ImmutableMap {
  def main(args: Array[String]): Unit = {

    val map1 = Map[String, Int]("zhangsan" -> 20, "lisi" -> 30)
    println(map1)

    val map2 = Map[String, Int](("ZHANGSAN", 30), ("LIST", 40))
    println(map2)

    //获取某个key分value值
    //println(map1("zhangsan1"))
    //Option 有两个子类
    // Some:代表有值，值封装在Some中
    // None：代表没有纸，代表提示外部进行处理
    //val value:Option[Int] = map1.get("zhangsan")
    //println(value.get)
    //getOrElse(k,默认值)
    println(map1.getOrElse("zhangsan1", "xxx"))

    //4、添加元素
    val map3 = map1.+(("wangwu", 40))
    val map4 = map1.+("wangwu" -> 40)
    println(map3)
    println(map4)

    val map5 = map1.++(Map[String, Int]("zhaoliu" -> 100, "aa" -> 1, "tianqi" -> 20))
    val map6 = map1.++:(Map[String, Int]("zhaoliu" -> 100, "aa" -> 1, "tianqi" -> 20))

    println(map5)
    println(map6)

    //删除
    val map7 = map1.-("zhangsan")
    println(map7)

    val map8 = map6.--(List("list", "tianqi", "aa"))
    println(map8)

    //如何遍历Map
    for (elemetn <- map8) {
      println(elemetn)
    }

    for ((key, value) <- map8) {
      println(s"key=${key} value=${value}")
    }

    //获取map所有key
    val keys = map8.keys
    for (key <- keys) {
      println(key)
    }

    //获取所有的value
    val values = map8.values
    for (value <- values) {
      println(value)
    }

  }
}



























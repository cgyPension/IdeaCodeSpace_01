package com.atguigu.charpter06

object $10_CollectionFunction {
  def main(args: Array[String]): Unit = {

    //基本属性与常用操作
    val list = List[Int](1,2,3)

    //查看集合长度
    println(list.length)
    println(list.size)

    //集合遍历
    for(i<-list){
      println(i)
    }

    //生成字符串
    println(list.mkString("#"))

    //是否包含
    println(list.contains(30))

  }
}

package com.atguigu.charpter07

import scala.collection.immutable.Queue

object $05_Math_List {
  def main(args: Array[String]): Unit = {
    val list = List("hello", false, 2.2, 10, 30, "hadoop")

    /*    list match {
      case List(x) => println("匹配list只有一个元素，${x}")
      case List(x:String,_:Boolean,_*) => println("匹配list至少有两个元素,这两个元素的类型为string boolean")
      case List(x,y,z,_*) => println(s"匹配list至少有三个元素 ${x} ${y} ${z}")
      case _ => println("其他")
    }*/

    var n: List[Any] = Nil //空的List
    n = list
    val list2 = 0 :: 10 :: Nil
    println(list2)

    list match {
      case x :: Nil => println(s"匹配list只有一个元素，${x}")
      case (s: String) :: (y: Boolean) :: tail => println(s"匹配至少两个元素，元素类型分别为string boolean ${tail}")
      case x :: y :: z :: tail => println(s"匹配list至少有三个元素 ${x} ${y} ${z} ${tail}")
    }

    /**
     * 泛型的特性：泛型擦除,不能用这种判断方式
     * List[Int] ->new List
     */

    list match {
      case x: List[Int] => println("List[Int]")
      case x: List[String] => println("List[String]")
      case x: List[Any] => println("List[Any]")
    }

    val map = Map[String, String]("aa" -> "bb")

    map match {
      case x: Map[String, Int] => println("Map[Int,Int]")
    }

    val set = Queue[Any](1, 2, 3)

    set match {
      case x: Queue[Int] => println("set[String]")
    }
  }
}

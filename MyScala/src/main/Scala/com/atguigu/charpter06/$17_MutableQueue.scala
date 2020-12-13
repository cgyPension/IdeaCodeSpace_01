package com.atguigu.charpter06

import scala.collection.mutable

object $17_MutableQueue {
  def main(args: Array[String]): Unit = {

    val queue = mutable.Queue[Int](2,4,1,5)

    //获取数据
    val value:Int = queue.dequeue()
    println(value)

    //添加数据
    println(queue.+:(10))
    queue.+=(20)
    println(queue)

    queue.+=:(30)
    println(queue)

    val queue2 = queue.:+(50)
    println(queue2)

    println(queue.++(List(10, 20)))

    println(queue.++:(List(100, 200)))

    queue.++=(List(100,200))
    println(queue)
  }
}

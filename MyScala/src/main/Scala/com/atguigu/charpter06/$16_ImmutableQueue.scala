package com.atguigu.charpter06

import scala.collection.immutable.Queue

object $16_ImmutableQueue {
  def main(args: Array[String]): Unit = {

    val queue = Queue[Int](2,5,1,3)

    val queue1 = queue.+:(7)
    println(queue1)

    val queue2 = queue.:+(10)
    println(queue2)
    val queue3 = queue.++(List(10,30,20))
    println(queue3)
    val queue4 = queue.++:(List(100,200))
    println(queue4)

    val queue6 =queue.enqueue(500)
    println(queue6)

    //获取元素把第一个元素拆出来，变成元组
    println(queue)
    val queue5 = queue.dequeue
    println(queue5)
  }
}

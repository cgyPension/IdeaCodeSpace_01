package com.atguigu.charpter06

object $17_PairList {
  def main(args: Array[String]): Unit = {
    val list = List[Int](2, 3, 4, 5, 67)

    list.foreach(x => {
      println(Thread.currentThread().getName)
      println(x)
    })

    println("=" * 100)
    list.par.foreach(x=>{
      println(Thread.currentThread().getName)
      println(x)
    })
  }
}

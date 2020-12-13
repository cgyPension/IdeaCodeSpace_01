package com.atguigu.charpter04

/**
 * 懒值加载: 在真正使用的时候才会赋予初始值
 *
 */
object $12_Lazy {
  def main(args: Array[String]): Unit = {

    def test():String ={
      println("xxxxxxxxxxxx")
      "zhangsan"
    }

    lazy val name = test()
    println("其他庞大功能")
    println("name="+name)
  }
}

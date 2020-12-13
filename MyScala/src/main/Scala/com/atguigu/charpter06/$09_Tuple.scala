package com.atguigu.charpter06



/**
 * 元组: 由()括起来的称之为元组
 * 定义：
 *     1、val t = (zhangsan,20,shenzhen)
 *     2、二元元组: K -> V
 * 元组一但定义，不但长度不可变，元素也不可改变
 *
 * 获取元组的值: 变量._角标[角标从1开始]
 */
object $09_Tuple {
  def main(args: Array[String]): Unit = {

    //School(name:String,age:Int)
    //Clazz(name:String,List[Student])
    val t1 = ("zhangsan",20,"shenzhen")
    //元组：
    //case class:

    val t2 = ("lisi",30)
    //kv结构就是指二元元组
    val t3 = "zhaoliu"->40

    //获取元组的值
    println(t1._1)
    println(t1._2)

    val t4 = ("zhangsan",20,"大数据班")
  }
}

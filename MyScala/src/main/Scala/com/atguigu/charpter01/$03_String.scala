package com.atguigu.charpter01

/**
 * 字符串创建:
 *   1、通过""包裹
 *   2、通过插值表达式
 *   3、通过""" """包裹
 *   4、format等方法
 */

object $03_String {
  def main(args: Array[String]): Unit = {
    //1、通过""包裹
    var name:String = "zhangsan"
    //2、插值表达式
    //val hello = "hello "+name
    val hello = s"hello ${name}"
    println(hello)

    //3、三引号【写sql语句】
    /*    val sql = "select " +
            "name," +
            "age," +
            "address " +
            "from xx"*/
    val tableName = "person"
    val sql =
      s"""
        |select
        |  name,
        |  age,
        |  adress
        |from ${tableName} a
        |  left join
        |  yy b
        |  on a.name = b.name
        |""".stripMargin

    println(sql)

    //4、一些方法
    //println(String.format("http://hadoop102:9000/%s","hello"))
   println("http://hadoop102:9000/%s".format("hello"))

  }
}

































































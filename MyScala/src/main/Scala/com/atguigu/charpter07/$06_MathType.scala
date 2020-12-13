package com.atguigu.charpter07

object $06_MathType {
  def main(args: Array[String]): Unit = {

    val tuple: (Any, Any, Any) = ("zhangsan", 20, "shenzhen")

    tuple match {
      case (name: String, _: Int, _: String) => println(s"${name} string")
      case (name, _, _) => println(s"name=${name}")
    }

    val school = List(
      ("爪哇中学", ("小学一班", ("1001", "zhangsan", 20))),
      ("爪哇中学", ("小学一班", ("1002", "lisi", 22))),
      ("爪哇中学", ("小学一班", ("1003", "wangwu", 23)))
    )

    //取出学生的姓名
/*    val result = school.map(_._2._2._2)
    val result2 = school.map( line =>{
       line match{
         case(schoolName,(className,(sid,sname,age))) =>sname
       }
    })*/

    val result = school.map{
      case (schoolName,(className,(sid,sname,age))) => sname
    }
    println(result)
  }
}

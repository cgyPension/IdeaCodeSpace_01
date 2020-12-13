package com.atguigu.charpter08

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Try
/**
 * java异常:
 *   1、抛异常
 *     1、方法里面抛异常: throw new Exception()
 *     2、方法抛异常: public void xx() throws Exception
 *   2、捕获异常
 *     try{
 *       ...
 *     }catch(Excetion e){
 *       ..
 *     }finally{
 *      ...
 *     }
 *
 * scala中方法不可以抛异常，没有throws关键字
 *
 * try{}catch{}finally{} 只在一种场景用到，使用外部链接的时候会用到
 */
object $01_Exception {
  def main(args: Array[String]): Unit = {

    //println(m1(2, 3))

    m1(2,5)

    val list = List[String](
      "1,zhangsan,20,shenzhen",
      "lisi,30,shanghai",
      "2,wangwu,30,shanghai"
    )

    println(list.map(line => {
      val arr = line.split(",")
      //val age = try{arr(2).toInt}catch {case e:Exception=>0}
      //
      val age: Try[Int] = Try(arr(2).toInt)
      age
    }).filter(_.isSuccess).map(_.get))

    val either = m3(5,0)

    either match {
      case Left(x) =>
        println(s"x=${x}")
      case Right((y,e)) =>
        println(y,e.getMessage)
    }

    if(either.isLeft)
      println(either.left.get)


  }

  def m2() = {

    var connection:Connection = null
    var statement:PreparedStatement = null

    try{
      connection = DriverManager.getConnection("jdbc:mysql://xx:3306/abc")

      statement = connection.prepareStatement("select * from xx where id=?")

      statement.setString(1,"1001")

      val set = statement.executeQuery()

      while (set.next()){
        val id = set.getString("id")
        println(s"id=${id}")
      }
    }catch {
      case e:Exception =>

    }finally {
      statement.close()

      connection.close()
    }

  }

  def m1(a:Int,b:Int): Int = {

    try {
      if (b == 5) throw new Exception("xxxx")
      a / b
    } catch {
      case e: Exception =>
        println(e.getMessage)
        0
    } finally {

    }
  }

  def m3(a:Int,y:Int) = {
    try {
      Left(a / y)
    } catch {
      case e: Exception =>
        Right(y,e)
    }
  }
}

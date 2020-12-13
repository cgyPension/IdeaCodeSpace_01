package com.atguigu.charpter04

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * java中方法的参数:
 *   1、类型 参数名
 *   2、类型... 参数名
 * scala中方法的参数
 *   1、参数名 参数类型
 *   2、默认值: 参数名 参数类型=值
 *   3、可变参数: 参数名 参数类型*
 *     可变参数不可以与默认值一起使用
 *     可变参数必须放在最后面
 *
 * 方法在调用的时候，可以通过带名参数给指定的参数进行赋值
 * 将数组/集合的所有元素放入可变参数中:   数组:_*
 */
object $03_MethodParam {
  def main(args: Array[String]): Unit = {
    println(add(2,3))

    println(add1(2,3))

    println(add2(3,3))
    println(add2(y=4))

    println(sum(2,3,4))

    xx(1,"hello",false)

    println("------------------")
    //add3(Array[Int](1,2,3,4))
    val paths = getPaths(7)

    readPath(paths:_*)
  }

  def add(x:Int,y:Int) = x + y
  def add1(x:Int = 0,y:Int = 0) = x + y
  def add2(x:Int=0,y:Int) = x + y

  def sum(y:Int,x:Int*) = x.sum
  def xx(x:Any*) = println(x.size)
  def add3(x:Int*) = println(x.size)

  //hdfs
  //path= /user/hive/warehouse/user_info/20200824
  //path= /user/hive/warehouse/user_info/20200823
  //path= /user/hive/warehouse/user_info/20200822
  //path= /user/hive/warehouse/user_info/20200821
  //path= /user/hive/warehouse/user_info/20200820
  //path= /user/hive/warehouse/user_info/20200819
  //path= /user/hive/warehouse/user_info/20200818
  //path= /user/hive/warehouse/user_info/20200817
  //path= /user/hive/warehouse/user_info/20200816

  def getPaths(day:Int) ={
    val prefix = "/user/hive/warehouse/user_info/"
    val format = new SimpleDateFormat("yyyMMdd")

    for(i<- 1 to day) yield{
      val date = new Date()
      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      calendar.add(Calendar.DAY_OF_YEAR,-i)
      val str = format.format(calendar.getTime)
      s"${prefix}${str}"
    }
  }

  def readPath(paths:String*)={

  }


}

































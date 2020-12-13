package com.atguigu.charpter01

/**
 * java中数据类型
 *     基本类型: byte、short、int、long、float、double、char、boolean
 *     引用类型: string、对象、集合、数组
 * scala是完全面向对象的语言
 *     Any: 所有类的父类
 *       AnyVal: 值类型
 *         Byte、Short、Int、Long、Float、Double、Char、Boolean
 *         Unit: 只有一个实例()，相当于java的void
 *         StringOps: 其实就是对java String的补充
 *       AnyRef: 引用类型
 *         String、集合、scala其他对象、java其他对象
 *             Null: 是所有引用类型的子类，只有一个实例null
 *
 *       Nothing: 所有类型的子类。没有实例，不能初始化。一般用于方法异常之后返回的数据
 *
 * 如果想用null对象引用类型赋予初始值，必须写明变量的类型
 *
 * 数字与字符串的转换
 *   1、数字转字符串
 *     1、插值表达式
 *     2、数字+""
 *     3、数字.toString
 *  2、字符串转数字： toXXX方法转换
 *
 * 数字间不同类型的转换
 *   1、精度小的能够自动转成精度大的
 *       val a:Int = 10
 *       val b:Long = a
 *   2、精度大转精度小的类型: toXXX
 *      val b:Long = 100
 *      val a:Int = b
 *
 */

object $05_DataType {
  def main(args: Array[String]): Unit = {

    val aa:Unit = ()

    "aaa %s".format("11")

    println("aa".splitAt(1))

    val bb:Null = null

    //多态
    //Person p = new Student
    //String name = new Null
    val name:String = null
    //Null cc = new Null
    var cc:String = null
    //cc = bew Strng(..)
    //Studnent s = new Person()
    cc = "zhangsan"
    //
    //val a:Int = null

    val dd = 10
    println(dd.toString)
    println(s"${dd}")

    //在字符串中 .不一定代表小数点
    "10.txt"
    //val ee = "10.1".toInt
    val ee = "10.1".toDouble
    println(ee)

    val b:Long = 100
    val a:Int = b.toInt
    println(a)
  }

}


































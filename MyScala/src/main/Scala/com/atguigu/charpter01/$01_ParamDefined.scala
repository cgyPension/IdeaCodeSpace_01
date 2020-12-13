package com.atguigu.charpter01

/**
 * 1、为什么main方法必须写在object中
 *    java中main定义: public static void main(String[] args){}
 *    main方法必须是静态的
 *  scala中没有static关键字
 *     scala中object中的所有的属性与方法都是类似静态的: 类名.方法,在编译时会产生2个class文件
 *     scala中class中的所有属性与方法都是非静态，在编译时，只会产生一个类文件
 * 2、def main(args: Array[String]): Unit 是啥
 *    main方法：
 *       def： defined的缩写，标识方法
 *       main: 方法名
 *       (args: Array[String]): 方法参数，参数名在前，类型在后，中间通过:分割
 *           User user = new User
 *           user.name
 *           user.age
 *           user.setName()
 *           ...
 *       参数列表后面跟上方法的返回值，参数列表与方法的返回值之间通过:分割
 *       Unit: 相当于java的void
 * 3、scala中是否需要分号
 *    如果一行只写一个语句，分号可以省略
 *    如果一行写多个语句，这多个语句之间必须通过;分割
 *
 */


// 单行注释
/* 多行注释 */
/** 文档注释 */

/**
 * scala中变量定义
 *   1、语法: val/var 变量名:变量类型 = 值
 *   2、val与var区别
 *     val定义的变量不可以被重新赋值,类似java的final效果
 *     var定义的变量可以被重新赋值
 *   3、scala中定义变量的时候可以省略类型，scala会自动推断变量类型
 *
 */

object $01_ParamDefined {
  def main(args: Array[String]): Unit = {

    val name:String = "zhangsan"

    var age:Int = 20

    //name = "list"

    age = 100

    println(name)
    println(age)

    val address = "shenzhen"
    println(address)
  }

}

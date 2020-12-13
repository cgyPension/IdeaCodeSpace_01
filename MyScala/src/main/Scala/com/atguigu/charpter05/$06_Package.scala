package com.atguigu.charpter05

//导入包下所有类: import 包名._
//import scala.collection.mutable._
//导入包下单个类: import 包名.类名
//import scala.collection.mutable.ArrayBuffer
//导入包下多个类: import 包名.{类名1,类名2,..}
//import com.atguigu.charpter04.{$01_Block, $02_if_else}

import java.util.{HashMap => _,_}
import scala.collection.mutable

object $06_Package {

  /**
   * java中package操作:
   *   1、声明包: 必须是文件的第一行
   *
   *   2、导包: 必须在class上面
   *     import java.util.*
   *     import java.util.XX
   *     import java.util.YY
   *     import java.util.ZZ
   *
   * scala中package操作
   *   1、声明包
   *     1、第一行声明包
   *     2、在文件中通过package 包名
   *   2、导入包
   *     1、导入包下所有类: import 包名._
   *     2、导入包下单个类: import 包名.类名
   *     3、导入包下多个类: import 包名.{类名1,类名2,..}
   *     4、导入包下类，并起别名: import 包名.{类名=> 别名}
   *     5、导入包下除开某个类的其他所有类: import java.util.{Date => _, _}
   *     scala可以在任何地方导包,如果不是在文件开头导包，那么其他地方导入包只能在当前作用域以及子作用域中使用
   *   3、包对象
   *     包对象中定义的方法与属性可以在包中任何地方使用
   *
   */
  def main(args: Array[String]): Unit = {

  }

  def m1() = {
    import java.util.{HashMap=>JavaHashMap}
    val func = () =>{
      //new JavaHashMap[]()
    }
  }
}

package xx{
  class Person

  object xx{

    def main(args:Array[String]):Unit = {
      println("..................")
    }

  }

}





















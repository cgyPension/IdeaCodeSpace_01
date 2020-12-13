package com.atguigu.charpter05

/**
 * 单例对象: 只有一个实例
 *
 * scala中单例对象就是object。如果想要创建一个单例对象，只需要创建一个object就可以了
 *
 * object中所有方法和属性都是类似java static修饰的,可以直接通过 object名称.方法名/属性名
 *
 * 伴生类与伴生对象
 *   1、class、object必须名字一样
 *   2、class、object必须在同一个源文件[.scala]中
 * 伴生类与伴生对象可以互相访问对方private修饰的属性与方法
 *
 * apply方法只能定义在伴生对象中，apply方法是用来简化伴生类的对象创建
 */
object $08_SingleObject {

  case class Person(name:String,age:Int)

  def main(args: Array[String]): Unit = {

    println($08_SingleObject)
    println($08_SingleObject)

    //println(AA.name)
    //println(AA.add(2,3))

   // val aa = new AA
    //aa.add2()

    val aa1 = AA("zhangsan",100)
    println(aa1.name)
    println(aa1.age)
    //val aa2 = AA()
    val arr = Array(1,2,3,4)
    aa1.hello()

    val person = Person("zhangsan",40)
  }


}


object AA{
  val name = "zhangsan"
  //伴生对象可以访问伴生类的私有属性与方法
  def add(x:Int,y:Int) = {
    val aa = new AA("",0)
    aa.add2(x,y)
    //AA.add2
  }

  private val address = "beijing"

  def apply(name:String,age:Int) = new AA(name,age)
}

class AA(val name:String,val age:Int){

  private def add2(x:Int,y:Int) = x * y
  //可以访问伴生对象的私有属性
  def hello() = println(AA.address)
}
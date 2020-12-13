package com.atguigu.charpter04



/**
 * 高阶函数: 参数/返回是函数的方法/函数
 * 高阶函数传参的简化:
 *   1、传入的函数可以省略参数类型
 *   2、如果函数只有一个参数，则()可以省略
 *   3、如果函数的参数在函数体中，只使用了一次，则可以通过_代替，下面两种情况除外
 *       1、如果函数参数只有一个，并且在函数体中直接返回参数本身，则参数不能用_代替
 *       2、如果函数体中有(),而且函数的参数处于函数体的()中，则不能用_代替
 *
 */
object $04_HightFunction {
  def main(args: Array[String]): Unit = {
    val func = (x:Int,y:Int) => x+y
    println(m1(2,3,func))

    val func1 = m2()
    println(func1(2,3))

    println(func10(func,2,3))

    println("------------------")
    m1(2,3,(x:Int,y:Int) => x+y)

    m1(2,3,(x,y) => x+y) //传入的时候可以省略参数类型

    //如果参数在函数体右边只使用了一次，那么可以将参数用_代替
    //如果函数只用一个参数，而且在函数体中直接返回参数的时候，该参数不能用_代替
    //如果函数体中有（），而且函数的参数处于（）中，侧参数不能用_代替
    m1(2,3,_+_)
    //如果函数的参数只有一个，那么（）可以省略
    m2((x:Int) => x*x)
    m2(x => x*x)  //此处不能使用_代替，因为x在函数体中使用了两次

    val func2 = (x:Int) => x
    m3(func2)
    m3((x:Int) => x)
    m3((x) => x)
    m3(_+1)  //x=>x+1
    println("-------------------------------")
    println(m3(_))  //m3(_)

  }


  def m1(a:Int,b:Int,func:(Int,Int) => Int) = {
    func(a,b)
  }

  def m2() = {
    val func = (x:Int,y:Int) =>x*y
    func
  }

  val func10 = (func:(Int,Int) => Int,a:Int,b:Int) =>func(a,b)

  def m2(func:Int => Int) = func(10)

  def m3(func:Int => Int) = func(10)
}

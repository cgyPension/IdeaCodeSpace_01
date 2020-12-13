package com.atguigu.charpter07

object $08_Math_Param {
  def main(args: Array[String]): Unit = {

    val tuple:(String,String,String) = m1(2,3)
    tuple match{
      case (x,y,z) => println("..")
    }
    //tuple._1
    val (t1,t2,t3) = m1(2,3)
    println(t1)
    println(t2)
    println(t3)

    val List(_,_,a,_*) = List(1,2,3,4,5)
    println(a)

    val Array(x,y,z) = Array(1,2,3)
    println(x,y,z)

    val map = Map[String,Int](("aa",1),("bb",2))

    for(i<- map) println(i)

    for((k,v)<-map) println(k,v)

  }


  def m1(x:Int,y:Int) = {
    println(s"${x} ${y}")
    ("*"*x,"+"*y,s"${x}+${y}=${x+y}")
  }
}

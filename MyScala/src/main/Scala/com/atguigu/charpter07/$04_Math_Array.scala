package com.atguigu.charpter07

object $04_Math_Array {
  def main(args: Array[String]): Unit = {

    //val arr = Array[Any] (10,20,30,40)
    val arr = Array[Any] (30,"hello",2.2,40)

    arr match {
      //匹配数组有三个元素
      case Array(x,y,z) =>
        println(s"数组有三个元素，${x} ${y} ${z}")
      //匹配数组首位元素
      case Array(10,_*) =>
        println("数组首位元素是10，后面的元素不清楚")
      case Array(x:Int,y:String,z:Double,_*) =>
        println("数组至少有三个元素，分为为int string double类型")
    }

    /**
     * Array[Int] => new Int[]{..}
     * Array[Stirng] => new String[]{}
     * arr = new Object[]{}
     *
     * new String[]{}
     */
    arr match{
      case x:Array[Any] =>println("Array[Any]")
    }
  }
}

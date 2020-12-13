package com.atguigu.charpter03

/**
 * java中for循环
 *   1、for(int i=0;i<=N;i++)
 *   2、for(String s:lists)
 *scala中的for循环类似java增强for循环
 *
 * to方法: 左右闭合
 *  调用方式:
 *   startIndex.to(endIndex)
 *   startIndex to endIndex
 * until方法: 左闭右开
 *   调用方式:
 *   startIndex.until(endIndex)
 *   startIndex until endIndex
 * scala中方法调用的两种方式:
 *   1、对象.方法(参数,...)
 *   2、对象 方法(参数,...)  //如果参数只有一个,()可以省略
 *
 * for循环基本语法:
 *   for(变量名 <-  表达式/集合/数组) { }
 * 守卫：在每次循环之前进行判断 for(变量名 <-  表达式/集合/数组 if (布尔表达式))
 * 嵌套for循环: for(变量名 <-  表达式/集合/数组;变量名 <-  表达式/集合/数组)
 *
 * 要想for循环有返回值，必须通过yeild表达式
 */
object $03_for {
  def main(args: Array[String]): Unit = {

    for(i<- 0 to 10){
      println(i)
    }

    println("="*20)//只能是字符串*数字，逻辑问题

    for(i<- 0 until 10){
      println("+"*i)
      if(i%2==0){
        println("*"*i)
      }
    }

    println("="*100)

    for(i<- 0 until 10 if i%2==0){
      println("*"*i)
    }

    //(0 until(10)).foreach(println(_))

    println("="*100)
    for(i<- 0 until (10)){
      if(i%2==0){
        val y=i*i
        for(j<- 0 until (y)){
          println(s"i*j=${i*j}")
        }
      }
    }

    println("="*100)
   val result = for(i<- 0 until (10) if i%2==0;y=i*i;j<- 0 until (y);k<- 0 until (j)){
      println(s"i*j*k=${i*j*k}")
      1+1
    }

    val result2 = for(i<- 1 to 10) yield{
      println(s"i=${i}")
      i*i
    }

    println(result2.toBuffer);

    for(i<- 0 until (10,2)) println(i)
    for(i<- 0 until (10) by 2) println(i)
    for(i<- (0 until (10)).by(2)) println(i)
  }
}













































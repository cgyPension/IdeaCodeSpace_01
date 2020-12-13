package com.atguigu.charpter03
import scala.util.control.Breaks._
/**
 * break:跳出循环
 * continue: 结束当次循环开始下一次循环
 * scala中没有break、continue关键字
 *
 * 要想实现跳出循环:
 *   1、return
 *   2、抛异常
 *
 * scala中要想实现break与continue功能
 *   1、导包: import scala.util.control.Breaks._
 *   2、通过breakable与break方法实现continue与break功能
 *
 * 1、实现continue
 *   for(...){
 *     breakable{
 *         if(..) break()
 *     }
 *   }
 * 2、实现break
 * breakable{
 *   for(...){
 *     if(..) break()
 *   }
 * }
 */
object $05_BreakContinue {
  def main(args: Array[String]): Unit = {

    //continue
    for(i<- 0 to 10){
      try {
        if (i % 2 == 0) throw new Exception
        println(i * i)
      } catch {
        case e:Exception =>
      }
    }

    println("="*100)
    for(i<- 0 to 10){
      breakable(
        {
         if(i%2==0) break()
         println(i*i)
        })
    }

    //break

    println("="*100)

    try {
      for (i <- 0 to 10) {
        if (i == 5) throw new Exception
        println(i * i)
      }
    } catch {
      case e:Exception =>
    }

    println("="*100)
    breakable({
      for(i<- 0 to 10){
        if(i==5) break()
        println(i*i)
      }
    })


  }
}
































































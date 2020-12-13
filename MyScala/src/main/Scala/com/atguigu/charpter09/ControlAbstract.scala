package com.atguigu.charpter09

/**
 * def breakable(op: => Unit) {
 * try {
 * op
 * } catch {
 * case ex: BreakControl =>
 * if (ex ne breakException) throw ex
 * }
 * }
 *
 *   Breaks.breakable{
 *     循环代码
 *
 *   }
 *
 *   def breakable(op: => Unit)   =======>   def breakable(op: ()=> Unit)
 *      参数传入的是一个函数，函数没有参数列表，返回Unit！
 *        函数的作用：  完成某一个功能
 *        op: 可以直接传入一个代码块
 *
 *        接受一段代码块，作为参数的函数称为控制抽象
 *
 *    控制抽象通常用于封装框架，将框架通用的逻辑进行封装，将用户自定义的逻辑，以代码块的形式传入执行！
 */
object ControlAbstract {

  //启动一个线程
  def main(args: Array[String]): Unit = {

    /*def startAThread(op: =>Unit)={
      new Thread(){
        override def run(): Unit = {
          println(Thread.currentThread().getName)
          //用户自己的逻辑
          op
        }
      }.start()
    }

    //调用
    startAThread({
      println("haha")
    })

    //调用
    startAThread{
      println("xixi")
    }*/

    /*
        mywhile 有两个参数列表

        mywhile(boolean)(用户自定义的逻辑)

        mywhile的作用： 根据第一个参数列表的返回值，来决定是否执行第二个参数列表的值！

        condition : (x,y) => Boolean
     */
    var i=0

    def mywhile(condition : => Boolean)(op: =>Unit):Unit={

      if (condition){
        op
        mywhile(condition)(op)
      }

    }

    mywhile(i<9){
      //逻辑
      println(i)
      i+=1
    }

  }
}

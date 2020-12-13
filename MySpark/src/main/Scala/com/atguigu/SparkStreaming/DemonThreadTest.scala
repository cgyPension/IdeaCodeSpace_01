package com.atguigu.SparkStreaming

class DemonThreadTest {
  /*
        守护线程

        如果JVM中只剩守护线程，此时JVM就会关闭！
   */
  def main(args: Array[String]): Unit = {

    val myThread = new MyThread

    val thread = new Thread(myThread, "守护线程")

    // 设置为守护线程
    thread.setDaemon(true)

    //启动了守护线程
    thread.start()

    Thread.sleep(1000)

    println("main线程先行告退!")

  }


}

class MyThread extends  Runnable {

  override def run(): Unit = {

    println(Thread.currentThread().getName+"  开始运行了！")

    for (i <- 1 to 10){

      println(Thread.currentThread().getName+"   "+i)
      Thread.sleep(500)

    }

  }
}

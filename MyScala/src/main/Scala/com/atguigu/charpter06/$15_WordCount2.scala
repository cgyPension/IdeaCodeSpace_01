package com.atguigu.charpter06



object $15_WordCount2 {
  def main(args: Array[String]): Unit = {
    val tupleList = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    //1、切割、压平
    tupleList.flatMap(x=>{
      val words = x._1.split(" ")
      //Array(Hello,Scala,Spark,World)
      words.map(word=>(word,x._2))
      //Array((Hello,4),(Scala,4),(Spark,4),(World,4))
    })
    //List((Hello,4),(Scala,4),(Spark,4),(World,4),(Hello,3),(Scala,3),(Spark,3),(Hello,2),(Scala,2).(Hello,1))

    //2、分组
      .groupBy(x=>x._1)
    //[
    // Hello->List((Hello,4),(Hello,3),(Hello,2),(Hello,1)),
    // Scala->List((Scala,4),(Scala,3),(Scala,2)),
    // Spark->List((Spark,4),(Spark,3)),
    // World->List((World,4))
    // ]

    //3、统计个数
      .map(x=>{
        //x=Hello->List((Hello,4),(Hello,3),(Hello,2),(Hello,1)),

        //val tuple = x._2.reduce((agg,curr)=>(agg._1,agg._2+curr._2))

        ////第一次执行 agg = (Hello,4)  curr=(Hello,3)   (agg,curr)=>(agg._1,agg._2+curr._2)  结果:(Hello,7)
        ////第二次执行 agg = (Hello,7)  curr=(Hello,2)   (agg,curr)=>(agg._1,agg._2+curr._2)  结果:(Hello,9)
        ////第三次执行 agg = (Hello,9)  curr=(Hello,1)   (agg,curr)=>(agg._1,agg._2+curr._2)  结果:(Hello,10)
        //tuple

        val word = x._1
        val sum = x._2.map(y=>y._2).sum
        //List(4,3,2,1)
        (word,sum)
      })

    //4、结果展示
    //[
    //  Hello->10,Scala->9,Spark->7,World->4
    // ]
      .foreach(println(_))
  }
}

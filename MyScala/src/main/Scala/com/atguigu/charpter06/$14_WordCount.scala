package com.atguigu.charpter06

import scala.io.Source

object $14_WordCount {
  def main(args: Array[String]): Unit = {

    //1、读取文件
    val data:List[String] = Source.fromFile("d:/wordcount.txt","utf-8").getLines().toList
    //List(
    //"hello python hello hadoop",
    //"hello scala hello java",
    //"java and scala and hadoop",
    //"flume hadoop kafka hbase",
    //"spark scala hadoop kafka",
    // )

    //flatten flatMap
    //2、切割、压平
    val words = data.flatMap(line=>line.split(" "))
    //List("hello","python","hello","hadoop",....)

    //3、分组
    val grouped = words.groupBy(x=>x)
    //[
    // (hello->List(hello,hello,hello,..)),
    // (python->List(python,python,..))
    // (java->List(java,java,..))
    //.....
    // ]

    //4、统计个数
    grouped.map(x=>{
      val word = x._1
      val sum = x._2.size
      (word,sum)
    }).foreach(println(_))


  }
}

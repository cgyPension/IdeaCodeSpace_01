package com.atguigu.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object RDD_Seria1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
    val sc = new SparkContext(sparkConf)

    // TODO Spark 序列化
    // 分布式运算   构成闭包后，检查sum是否支持序列化，如果支持序列化，
    // 此时创建sum的一个副本，将副本发送到Task执行的Executor，在executor上运算
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    /*  rdd.foreach(num => {
      val user = new User()
      println("age =" + (user.age + num))
    })*/

    // TODO Exception : Task not serializable
    // 如果算子中使用了算子外的对象，那么在执行时，需要保证这个对象能序列化。
/*    val user = new User()
    rdd.foreach(num=>{
        println("age = "+ (user.age + num))
    })*/

    // TODO Scala 闭包
    val user1 = new User()
    val rdd2: RDD[Int] = sc.makeRDD(List())

    // Spark的算子的操作其实都是闭包，所以闭包有可能包含外部的变量
    // 如果包含外部的变量，那么就一定要保证这个外部变量可序列化。
    // 所以Spark在提交作业之前，应该对闭包内的变量进行检测，检测是否能够序列化
    // 将这个操作为闭包检测
    rdd2.foreach(num=>{
      println( "age = " + (user1.age + num) )
    })



  }

  class User extends Serializable{
    val age:Int = 20
  }


 /* class User {
    val age:Int = 20
  }*/

  // 样例类自动混入可序列化特质
  //case class User(age:Int = 20)
}

package com.atguigu.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_dep1 {
  def main(args: Array[String]): Unit = {
    // todo Spark 依赖关系

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    //List()   rdd是初始RDD，并不是由其他的RDD转换得来，dependencies指当前RDD依赖于哪些RDD的依赖关系
    println(rdd.dependencies)

    val rdd1: RDD[Int] = rdd.map(x => x)
    val rdd2 = rdd1.map(x => x)
    val rdd3 = rdd2.map(x => x)
    val rdd4 = rdd3.map(x => x)
    val rdd5 = rdd4.map(x => x)
    val rdd6 = rdd5.map(x => (x,1))

    // List(org.apache.spark.OneToOneDependency@5524b72f)   1对1的依赖  指父RDD的一个分区 经过转换后，所有的元素都会 放入子RDD的一个分区
    // OneToOneDependency 是窄依赖的一种表现   OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency
    println(rdd6.dependencies)

    val result: RDD[(Int, Int)] = rdd6.reduceByKey(_ + _)

    //查看血缘
    println(result.toDebugString)

    println("--------------------------------------")

    //查看依赖  List(org.apache.spark.ShuffleDependency@599e4d41)  宽依赖
    println(result.dependencies)


    // Error
    // 如果Spark的计算过程中某一个节点计算失败，那么框架会尝试重新计算
    // Spark既然想要重新计算，那么就需要知道数据的来源，并且还要知道数据经历了哪些计算
    // RDD不保存计算的数据，但是会保存元数据信息


    sc.stop()
  }
}

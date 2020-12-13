package com.atguigu.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}



/**
 *
 * 1.行动算子和转换算子的区别
 *        转换算子将一个RDD转换为另一个RDD,是懒执行！
 *        行动算子，是提交Job
 *
 * 2.reduce: 提供一个函数，将RDD中元素两两运算！
 * 3.collect：  将RDD中的元素，以Array的形式，收集到Driver端
 *                  只有当RDD中的元素数据较少时，才可以调用！否则Driver端可能OOM
 * 4.count: 返回RDD中元素的个数
 * 5.first： 返回RDD中的第一个元素。
 *              第一个分区的第一条数据
 * 6.take:  按照分区依次取出前N个元素
 * 7.takeOrdered: 先排序，之后依次取出前N个元素
 *
 *              take和takeOrdered都需要注意 取的数据不能太多，否则Driver会OOM
 *
 * 8.aggregate:  和aggregateByKey的区别，在于zeroValue不仅会在分区内参与运算，还会在分区间参与运算！
 *
 * 9.fold:  fold是aggregate的简化版，如果分区内和分区间运算逻辑一致，可以简化使用fold
 * 10.countByKey:   必须是Key-value类型的rdd才能使用，统计相同key对应的k-v对的个数
 * 11.countByValue：  统计相同元素的个数
 * 12.save相关 :
 * 13.foreach ： 在特殊的场景下，使用低效！
 *                  将RDD中的每个元素写入数据库！
 *                    建议使用foreachPartition ,一个分区作为一批数据进行处理！
 *
 *                    map =====> mapPartition
 */
class RDD_operator_action {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("RDD_operator"))

  // TODO Spark的行动算子执行时，会产生Job对象，然后提交这个Job对象
  // 所谓的行动算子，其实不会再产生新的RDD，而是触发作业的执行
  // 行动算子执行后，会获取到作业的执行结果。
  // 转换算子不会触发作业的执行，只是功能的扩展和包装。
  @Test
  def test1(): Unit = {

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO reduce
    // 简化，规约
    val i: Int = rdd.reduce(_ + _)
    println(i)

    // TODO collect  采集数据
    // collect方法会将所有分区计算的结果拉取到当前节点Driver的内存中，可能会出现内存溢出
    val data: Array[Int] = rdd.collect()
    data.foreach(println)

    // TODO count
    val cnt: Long = rdd.count()

    // TODO first
    val f: Int = rdd.first()

    // TODO take
    val subarrat: Array[Int] = rdd.take(3)

    // TODO takeOrdered
    // 1,2,3,4 => 1,2,3
    // 2,1,4 => 1,2,4
    val rdd1: RDD[Int] = sc.makeRDD(List(2,1,4,3))
    val ints: Array[Int] = rdd1.takeOrdered(3)
    println(ints.mkString(","))
  }


  @Test
  def test2(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // TODO sum
    val d: Double = rdd.sum()
    println(d)

    // TODO aggregate
    // aggregateByKey: 初始值只参与到分区内计算
    // aggregate: 初始值分区内计算会参与，同时分区间也会参与
    val i: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(i)

    // TODO fold
    val i1: Int = rdd.fold(10)(_ + _)
    println(i1)

    // TODO countByKey - 7
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("a", 1), ("a", 1)
    ))

    val wordToCount: collection.Map[String, Long] = rdd2.countByKey()
    println(wordToCount)

    // TODo countByValue - 8
    val rdd3: RDD[String] = sc.makeRDD(List(
      "a", "a", "a", "hello", "hello"
    ))

    val wordToCount1: collection.Map[String, Long] = rdd3.countByValue()
    println(wordToCount1)
  }


  @Test
  def testsave(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    //  序列化后的 K-V   只有K-V类型的RDD才能调用
    rdd.map((_,1)).saveAsSequenceFile("output2")

  }

  // 本地模式以多线程的形式模拟多个Task执行
  @Test
  def testforeach(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // todo foreach 算子
    // rdd的方法称之为算子
    // 算子的逻辑代码是在分布式计算节点Executor执行的
    // 无序,foreach算子可以将循环在不同的计算节点完成
    // 算子之外的代码是在Driver端执行
    rdd.foreach(x=>println(Thread.currentThread().getName+":"+x))

    // rdd.foreachPartition()

    println("------------------------")

    // todo foreach 方法
    // 有序,集合的方法中的代码是在当前节点(Drive)中执行的
    // foreach方法是在当前节点的内存中完成数据的循环
    rdd.collect().foreach(x=>println(Thread.currentThread().getName+":"+x))

  }

  @After
  def stop(): Unit = {
    sc.stop()
  }
}

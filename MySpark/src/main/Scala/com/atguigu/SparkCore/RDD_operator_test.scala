package com.atguigu.SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

class RDD_operator_test {
  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("RDD_operator"))


  // TODO 统计出每一个省份每个广告被点击数量排行的Top3
  @Test
  def test1(): Unit = {

    // 1、获取数据
    val dataRDD = sc.textFile("input/agent.log")

    // 2、将原始数据进行结构的转换，方便统计
    //  (（省份 - 广告）,1)
    val mapRDD : RDD[(String, Int)] = dataRDD.map(
      line => {
        val datas = line.split(" ")
        (datas(1) + "-" + datas(4),1)
      }
    )

    // 3、将相同key的数据进行分组聚合
    //  (（省份 - 广告）,sum)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    // 4、将聚合后的结果进行结构转换
    //  ( 省份，（广告，sum） )
    val mapRDD1 = reduceRDD.map{
      case ( key, sum ) => {
        val keys = key.split("-")
        ( keys(0), ( keys(1), sum ) )
      }
    }

    // 5、将相同的省份的数据分在一个组中
    //  ( 省份，Iterator[（广告1，sum1）,（广告2，sum2）] )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

    // 6、将分组后的数据进行排序（降序），取前三
    // Scala mapValues
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(3)
    })

    // 7、数据采集到控制台打印
    val result: Array[(String, List[(String, Int)])] = sortRDD.collect()
    result.foreach(println)
  }

  // TODO Spark - 案例实操
  @Test
  def test2(): Unit = {

    //        val rdd: RDD[Int] = sc.makeRDD(List(1,1,2,2),2)
    //        // 1->2, 2->2
    //        val result = rdd.mapPartitions(
    //            iter => {
    //                //val len = iter.length
    //                //iter.hasNext
    //                // 迭代器
    //                //List((iter.next(), len)).iterator
    //                iter
    //            }
    //        )
    //
    //        println(result.collect().mkString(","))

    val iter: Iterator[Int] = List(1, 2, 3, 4).iterator
    val len: Int = iter.length
    while (iter.hasNext){
      println(iter.next())
    }
  }

   // todo 分区内同时求最大和最小，分区间合并
    @Test
    def testAggregateByKeyExec2()={
      val list = List((1, 1), (1, 2), (2, 1), (1,3),(2,2),(2, 2))
      val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

      val result: RDD[(Int, (Int, Int))] = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
        { case ((min, max), value) => (min.max(value), max.min(value)) },
        { case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2) })

      result.saveAsTextFile("output")
    }

  // todo  求每个key对应的平均值 平均值=  sum / count
  @Test
  def testAggregateByKeyExec3() : Unit ={
    val list = List((1, 1), (1, 2), (2, 1), (1,3),(2,2),(2, 2))
    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)

    val rdd2: RDD[(Int, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      { case ((sum, count), value) => (sum + value, count + 1) },
      { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
    )

    val result: RDD[(Int, Int)] = rdd2.map {
      case (key, (sum, count)) => (key, sum / count)
    }

    result.saveAsTextFile("output")
  }


  // todo 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值
  @Test
  def testCombineByKeyExec() : Unit ={

    val list = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))

    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    /* val rdd2: RDD[(Int, (Int, Int))] = rdd.aggregateByKey((0, 0))(
       { case ((sum, count), value) => (sum + value, count + 1) },
       { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) })

     val result: RDD[(Int, Int)] = rdd2.map {
       case (key, (sum, count)) => (key, sum / count)
     }*/

    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey(value => (value, 1),
      { case ((sum: Int, count: Int), value) => (sum + value, count + 1) },
      { case ((sum1: Int, count1: Int), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
    )

    val result: RDD[(String, Double)] = rdd2.map {
      case (key, (sum, count)) => (key, sum.toDouble / count)
    }

    result.saveAsTextFile("output")
  }



  @After
  def stop(): Unit = {
    sc.stop()
  }
}































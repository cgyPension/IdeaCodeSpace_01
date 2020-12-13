package com.atguigu.SparkCore.exc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


// 需求2：Top10热门品类中每个品类的Top10活跃Session统计
object Function2Demo1App extends BaseApp{

  override val pathName: String = "MySpark/output/Function2Demo1App"

  def main(args: Array[String]): Unit = {
      runApp{
        //求出前十热门品类
        val rdd1: RDD[String] = sparkContext.textFile("MySpark/output/Function1Demo1App")

        val rdd2: RDD[String] = rdd1.map(line => {
          val words: Array[String] = line.split(",")
          words(3).substring(0, words(3).length - 1)
        })

        val top10HotCatogory: Array[String] = rdd2.collect()

        //使用广播变量
        val br: Broadcast[Array[String]] = sparkContext.broadcast(top10HotCatogory)

        //将前十人们品类的数据进行过滤
        val rdd3: RDD[UserVisitAction] = getAllData()
        val rdd4: RDD[UserVisitAction] = rdd3.filter(bean => br.value.contains(bean.click_category_id.toString))

        //计算每个品类，每个品类，所有session的总点击数
        val rdd5: RDD[(Long, Iterable[(String, Int)])] = rdd4.map(bean => ((bean.click_category_id, bean.session_id), 1))
          .reduceByKey(_ + _)
          .map {
            case ((click_category_id, session_id), count) => (click_category_id, (session_id, count))
          }
          .groupByKey()

        //将每个品类，取session点击数前十
        val result: RDD[(Long, List[(String, Int)])] = rdd5.mapValues(iter => iter.toList.sortBy(-_._2).take(10))
        result.coalesce(1).saveAsTextFile(pathName)
      }
  }
}

package com.atguigu.SparkCore.exc

import java.text.DecimalFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

//需求3：页面单跳转换率统计
object Function3Demo1App extends BaseApp{
  override val pathName: String = "MySpark/output/Function3Demo1App"

  def main(args: Array[String]): Unit = {
    runApp{

      //计算所有页面的访问次数
      val rdd: RDD[UserVisitAction] = getAllData()
      val rdd2: RDD[(Long, Int)] = rdd.map(bean => (bean.page_id, 1))
        .reduceByKey(_+_)


      // page view
      val pvMap: Map[Long, Int] = rdd2.collect().toMap

      //使用广播变量
      val br: Broadcast[Map[Long, Int]] = sparkContext.broadcast(pvMap)

      println("--------------------------计算页面单跳的次数--------------------")

      val rdd3: RDD[((Long, String), Iterable[(Long, String)])] = rdd.map(bean => ((bean.user_id, bean.session_id), (bean.page_id, bean.action_time)))
        .groupByKey()


      //List((页面id，时间1),(页面id，时间2)..)
      val rdd4: RDD[List[(Long, String)]] = rdd3.values.map(iter => iter.toList.sortBy(_._2))


      /*
         List(页面1,页面2...)

         List(A,B,C,D)
         List(B,C,D)  : tail(非头即尾)

         scala集合中的zip：  (A,B)(B,C)(C,D)
    */

      val pagesRDD: RDD[List[Long]] = rdd4.map(list => list.map(x => x._1))



      //获取到所有的单跳页面
      val singeJumpRDD: RDD[(Long, Long)] = pagesRDD.flatMap(list => list.zip(list.tail))



      //统计每个单跳页面的总次数
      val singeJumpSumRDD: RDD[((Long, Long), Int)] = singeJumpRDD.map(x => (x, 1)).reduceByKey(_ + _)

      singeJumpSumRDD.foreach(println)

      val format = new DecimalFormat("0.00%")

      //计算单跳转换率
      singeJumpSumRDD.map{
        case ((from,to),count) => from+"->"+":"+format.format(count.toDouble/br.value.getOrElse(from,1))
      }.coalesce(1).saveAsTextFile(pathName)
    }
  }
}

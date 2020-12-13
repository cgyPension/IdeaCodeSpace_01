package com.atguigu.SparkCore.exc

import org.apache.spark.rdd.RDD

object Function1Demo1App extends  BaseApp{
  override val pathName: String = "MySpark/output/Function1Demo1App"

  def main(args: Array[String]): Unit = {
    runApp{

      //全量数据
      val rdd: RDD[UserVisitAction] = getAllData()

      //只需要统计下单，点击和支付 ,将搜索数据进行过滤
      val rdd1: RDD[UserVisitAction] = rdd.filter(bean => bean.search_keyword.equals("null"))

      //分别统计下单，点击和支付
      //点击
      val clickRDD: RDD[(Long, Int)] = rdd1.filter(bean => bean.click_category_id != -1 || bean.click_product_id != -1)
        .groupBy(bean => bean.click_category_id)
        .map {
          case (category_id, iters) => (category_id, iters.size)
        }

      //下单
      val orderRDD: RDD[(Long, Int)] = rdd1.filter(bean => bean.order_category_ids != "null" || bean.order_product_ids != "null")
        .flatMap(bean => {
          val category_ids: Array[String] = bean.order_category_ids.split(",")
          for (category_id <- category_ids) yield (category_id.toLong, 1)
        }).reduceByKey(_ + _)



      //支付
      val payRDD: RDD[(Long, Int)] = rdd1.filter(bean => bean.pay_category_ids != "null" || bean.pay_product_ids != "null")
        .flatMap(bean => {
          val category_ids: Array[String] = bean.pay_category_ids.split(",")
          for (category_id <- category_ids) yield (category_id.toLong, 1)
        }).reduceByKey(_ + _)

      //leftJoin

      val rdd2: RDD[(Long, ((Int, Option[Int]), Option[Int]))] = clickRDD.leftOuterJoin(orderRDD).leftOuterJoin(payRDD)

      val joinRDD: RDD[((Int, Int, Int), Long)] = rdd2.map {
        case (category_id, ((cc, oc), pc)) => ((cc, oc.getOrElse(0), pc.getOrElse(0)), category_id)
      }

      //排序
      val result: RDD[((Int, Int, Int), Long)] = joinRDD.sortByKey(false, 1)

      sparkContext.makeRDD(result.take(10),1).saveAsTextFile(pathName)

    }
  }
}

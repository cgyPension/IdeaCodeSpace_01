package com.atguigu.SparkCore.exc

import org.apache.spark.rdd.RDD


object Function1Demo2App extends  BaseApp {
  override val pathName: String = "MySpark/output/Function1Demo2App"

  def main(args: Array[String]): Unit = {

    runApp{
      val rdd: RDD[UserVisitAction] = getAllData()

      //只需要统计下单，点击和支付，将搜索数据进行过滤
      val rdd1: RDD[UserVisitAction] = rdd.filter(bean => bean.search_keyword.equals("null"))

      //union
      val JoinRDD: RDD[(Long, (Int, Int, Int))] = rdd1.flatMap(bean => {
        //判断是否是点击数据
        if (bean.click_category_id != -1 || bean.click_category_id != -1) {
          //封装记录
          List((bean.click_category_id, (1, 0, 0)))
        } else if (bean.order_category_ids != "null" || bean.order_product_ids != "null") {
          //判断是否是下单数据
          val category_ids: Array[String] = bean.order_category_ids.split(",")
          for (category_id <- category_ids) yield (category_id.toLong, (0, 1, 0))
        } else if (bean.pay_category_ids != "null" || bean.pay_product_ids != "null") {
          val category_ids: Array[String] = bean.pay_category_ids.split(",")
          for (category_id <- category_ids) yield (category_id.toLong, (0, 0, 1))
        } else {
          Nil
        }
      })

      //groupby ---> sum
      val reduceRDD: RDD[(Long, (Int, Int, Int))] = JoinRDD.reduceByKey {
        case ((cc1, oc1, pc1), (cc2, oc2, pc2)) => (cc1 + cc2, oc1 + oc2, pc1 + pc2)
      }

      //排序取前10
      sparkContext.makeRDD(reduceRDD.map{
        case (k,v) =>(v,k)
      }.sortByKey(false,1).take(10),1).saveAsTextFile(pathName)

    }

  }
}

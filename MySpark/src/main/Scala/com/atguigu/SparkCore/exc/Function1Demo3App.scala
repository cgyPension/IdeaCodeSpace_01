package com.atguigu.SparkCore.exc

import org.apache.spark.rdd.RDD


// 需求1：Top10热门品类
object Function1Demo3App extends BaseApp {

  override val pathName: String = "MySpark/output/Function1Demo3App"

  def main(args: Array[String]): Unit = {

    runApp{
      val acc = new Top10HotCategoryAcc
      sparkContext.register(acc,"Top10")
      val rdd: RDD[UserVisitAction] = getAllData()

      //只需要统计下单，点击和支付 ,将搜索数据进行过滤
      val rdd1: RDD[UserVisitAction] = rdd.filter(bean => bean.search_keyword.equals("null"))

      // 累加
      rdd1.foreach(bean =>{
        //判断是否是点击数据
        if(bean.click_category_id != -1 || bean.click_product_id != -1) {
          //封装记录
          acc.add((bean.click_category_id.toString,"cc"))
        }else if(bean.order_category_ids != "null" || bean.order_product_ids != "null"){
          //判断是否是下单数据
          val category_ids: Array[String] = bean.order_category_ids.split(",")
          for(category_id <- category_ids) acc.add((category_id,"oc"))
        }else if(bean.pay_category_ids !="null" || bean.pay_product_ids != "null"){
          val category_ids: Array[String] = bean.pay_category_ids.split(",")
          for(category_id <- category_ids) acc.add((category_id,"pc"))
        }else{
          Nil
        }
      })

      val toSort: List[CategoryInfo] = acc.value.values.toList

      val result: List[CategoryInfo] = toSort.sortBy(x => x).take(10)

      sparkContext.makeRDD(result,1).saveAsTextFile(pathName)
    }

  }

}

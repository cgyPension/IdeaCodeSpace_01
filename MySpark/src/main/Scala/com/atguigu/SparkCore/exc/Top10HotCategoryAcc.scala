package com.atguigu.SparkCore.exc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 *  输入：  品类ID，标记(累加的值的类型)
 *                cc: 点击数据
 *                oc: 下单数据
 *                pc: 支付数据
 *
 *            ()
 *
 *
 *  输出：   {(品类1，点击数，支付数，下单数) ...  }
 *          Map[ String ,CategoryInfo]
 *
 */
class Top10HotCategoryAcc extends AccumulatorV2[Tuple2[String,String],mutable.Map[String,CategoryInfo]]{
  val result: mutable.Map[String, CategoryInfo] = mutable.Map[String, CategoryInfo]()

  override def isZero: Boolean = result.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, CategoryInfo]] = new Top10HotCategoryAcc

  override def reset(): Unit = result.clear()

  //累加 点击数，支付数，下单数
  override def add(v: (String, String)): Unit = {
    if(v._2.equals("cc")){
      val bean: CategoryInfo = result.getOrElse(v._1, CategoryInfo(v._1, 0, 0, 0))
      bean.cc +=1
      result.put(v._1,bean)
    }else if(v._2.equals("oc")){
      val bean: CategoryInfo = result.getOrElse(v._1, CategoryInfo(v._1, 0, 0, 0))
      bean.oc += 1
      result.put(v._1,bean)
    }else{
      val bean: CategoryInfo = result.getOrElse(v._1, CategoryInfo(v._1, 0, 0, 0))
      bean.pc += 1
      result.put(v._1,bean)
    }

  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, CategoryInfo]]): Unit = {
    val toMergeMap: mutable.Map[String, CategoryInfo] = other.value
    for (categoryInfo<-toMergeMap.values){
      val bean1: CategoryInfo = result.getOrElse(categoryInfo.id, CategoryInfo(categoryInfo.id, 0, 0, 0))
      bean1.oc += categoryInfo.oc
      bean1.cc += categoryInfo.cc
      bean1.pc += categoryInfo.pc

      result.put(bean1.id,bean1)
    }
  }

  override def value: mutable.Map[String, CategoryInfo] = result

}

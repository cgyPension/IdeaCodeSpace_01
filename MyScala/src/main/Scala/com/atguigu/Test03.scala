package com.atguigu

import scala.io.Source
import java.text.SimpleDateFormat

/**
 * 1、求出哪些省份没有农产品市场
 * 2、获取农产品种类最多的三个省份
 * 3、获取每个省份农产品最多的三个农贸市场
 */

object Test03 {
  def main(args: Array[String]): Unit = {

    //读数据
    val products = Source.fromFile("D:\\Program Files\\feiq\\Recv Files\\product.txt","utf-8").getLines().toList
    val allprovince = Source.fromFile("D:\\Program Files\\feiq\\Recv Files\\allprovince.txt","utf-8").getLines().toList
    //home1(allprovince,products)
   home2(products)
    //home3(products)
  }

  /**
   *   1、求出哪些身份没有农产品市场
   */
  def home1(allprovince:List[String],products:List[String]): Unit ={

    //3、从产品数据中取出省份字段
    val productProvinces = products
      .filter(_.split("\t").size==6)
      .map(line=>{
        val arr = line.split("\t")
        val province = arr(4)
        province
      })

    //4、去重
      .distinct

    //5、求差
    allprovince.diff(productProvinces)
    //6、展示结果
      .foreach(println(_))
  }

  /**
   * 2、获取农产品种类最多的三个省份
   */
  def home2(products:List[String]): Unit ={
    //1、进行过滤、列裁剪、去重
    products.filter(_.split("\t").size==6)
      .map(line=>{
        val arr = line.split("\t")
        val name = arr.head
        val province = arr(4)
        (province,name)
      })
      .distinct
    //2、按照省份进行分组，统计每个省份对应的种类
      .groupBy(_._1)
      .map(x=>{
        val province = x._1
        val data = x._2
        val num = x._2.size
        (province,num)
      })
    //3、取得前三
      .toList  //map不能排序底层是链表要转为List
      .sortBy(_._2)
      .takeRight(3)
    //加过展示
      .foreach(println(_))
  }

  /**
   * 3、获取每个省份农产品最多的三个农贸市场
   */
  def home3(products: List[String]): Unit ={
    //1、进行过滤、列裁剪、去重
    products.filter(_.split("\t").size==6)
      .map(line=>{
        val arr = line.split("\t")
        val name = arr.head
        val market = arr(3)
        val province = arr(4)
        (province,market,name)
      })
      .distinct
    //2、按照省份+农贸市场进行分组
      .groupBy(x=>(x._1,x._2))
    /**
     * [
     *    (山西,山西汾阳市晋阳农副产品批发市场)  -> List((山西,山西汾阳市晋阳农副产品批发市场,香菜),(山西,山西汾阳市晋阳农副产品批发市场,芹菜),..)
     *    ..
     * ]
     *
     */

    //3、统计每个省份、每个农贸市场的菜的种类
      .map{
        case ((province,market),list)=>
          (province,market,list.size)
      }

    //4、按照省份进行分组
      .groupBy(_._1)
    /**
     * [
     *   山西 -> List((山西,山西汾阳市晋阳农副产品批发市场,10),(山西,AA,20),(山西,BB,5),..)
     * ]
     */
    //5、取得每个省份前三
      .map(x=>{
        val province =x._1
        val arr = x._2.toList.sortBy(_._3).takeRight(3)
        (province,arr)
      })
    //6、展示结果
      .foreach(println(_))
  }
}






























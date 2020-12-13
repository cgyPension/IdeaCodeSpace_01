package com.atguigu.SparkSql.req

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Req1_Mock {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // todo 创建环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // todo 访问外置的Hive
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse")
      .config(sparkConf).getOrCreate()

    spark.sql("show databases").show


   //spark.sql("create database xianmu1")
    spark.sql("use xianmu1")

    spark.sql("show tables").show

    //spark.sql("select * from user_visit_action").show

    //spark.sql("desc  table extended user_visit_action").show()
/*
    spark.sql(
      """
        |create table `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
            """.stripMargin)
*/


    spark.sql(
      """
        |load data  inpath 'hdfs://hadoop102:9820/input/user_visit_action.txt' into table user_visit_action
        |""".stripMargin)

   /* spark.sql(
      """
        |create table `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'MySpark/input/product_info.txt' into table product_info
            """.stripMargin)


    spark.sql(
      """
        |create table `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    spark.sql(
      """
        |load data local inpath 'MySpark/input/city_info.txt' into table city_info
            """.stripMargin)

    spark.sql(
      """
        |select*from city_info
        |""".stripMargin)*/

    spark.stop
  }
}

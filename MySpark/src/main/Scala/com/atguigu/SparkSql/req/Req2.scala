package com.atguigu.SparkSql.req

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Req2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // todo 创建环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // todo 访问外置的Hive
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse")
      .config(sparkConf).getOrCreate()

    spark.sql("use xianmu1")

    spark.sql(
      """
        |select
        |   *
        |from (
        |    select
        |        *,
        |        rank() over( partition by area order by clickCount desc ) as rank
        |    from (
        |        select
        |            area,
        |            product_name,
        |            count(*) as clickCount
        |        from (
        |            select
        |               a.*,
        |               c.area,
        |               p.product_name,
        |               c.city_name
        |            from user_visit_action a
        |            join city_info c on c.city_id = a.city_id
        |            join product_info p on p.product_id = a.click_product_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3
        |where rank <= 3
            """.stripMargin).show

    spark.stop()
  }
}

package com.atguigu.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit._

class Load_Hive {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")

  // TODO 默认情况下SparkSQL支持本地Hive操作的，执行前需要启用Hive的支持
  // 调用enableHiveSupport方法。
  /*
      val spark: SparkSession = SparkSession.builder()
        .enableHiveSupport()
        .config(conf)
        .getOrCreate()
  */
  // TODO 访问外置的Hive
  val spark: SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse")
    .config(conf).getOrCreate()

  import spark.implicits._

  @Test
  def test() : Unit ={

    spark.sql("show databases").show
    spark.sql("desc database extended default").show()
    spark.close()

   /* spark.sql("create table aa(id int)")
    spark.sql("show tables").show()

    spark.sql("load data local inpath 'input/id.txt' into table aa")
    spark.sql("select*from aa").show

    val list = List(Person("jack", 20), Person("marry", 21))
    val rdd: RDD[Person] = spark.sparkContext.makeRDD(list, 1)


    val ds: Dataset[Person] = rdd.toDS()
    // Insert into
    ds.write.mode("append").saveAsTable("mydb4.t2")

    // insert overwrite
    ds.write.mode("overwrite").saveAsTable("mydb4.t2")*/

  }

  @After
  def stop() {

    spark.close()
  }
}

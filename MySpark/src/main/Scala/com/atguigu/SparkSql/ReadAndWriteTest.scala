package com.atguigu.SparkSql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.junit._
class ReadAndWriteTest {

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import sparkSession.implicits._

  /**
   *    数据以文件形式存在
   *          Json：
   *          CSV： 逗号分割
   *          TSV： TAB分割
   */

  // TODO SparkSQL读取和保存
  @Test
  def readandwrite() : Unit ={

    // TODO 读取
    // TODO SparkSQL通用读取的默认数据格式为Parquet列式存储格式
    //  val frame: DataFrame = spark.read.load("input/users.parquet")

    // TODO 特定读取
    // JSON文件的格式要求整个文件满足JSON的语法规则
    // Spark读取文件默认是以行为单位来读取。
    // Spark读取JSON文件时，要求文件中的每一行符合JSON的格式要求
    // 如果文件格式不正确，那么不会发生错误，但是解析结果不正确
    val df1: DataFrame = sparkSession.read.json("input/employees.json")
    df1.show

    // TODO 通用读取
    val df2: DataFrame = sparkSession.read.format("csv").load("input/user.csv")
    df2.show

    val df3: DataFrame = sparkSession.read.format("csv")
      .option("sep", ";")     // 设置分隔符
      .option("inferSchema", "true")
      .option("header", "true")    //设置是否讲第一列解析为列名
      .load("input/user1.csv")
    df3.show

    // TODO 保存
    // TODO sparksql默认通用保存的文件格式为parquet

    // TODO 特定保存


    // TODO 通用保存
    // 如果路径已经存在，那么执行保存操作会发生错误。
    //df2.write.format("csv").save("output")

    /**
     *    saveMode : 写出数据的模式！
     *        默认：  SaveMode.ErrorIfExists
     *                  如果输出路径存在，就报错！
     *        overwrite:  覆盖写
     *        append： 追加写，向输出目录追加写文件
     *        ignore： 忽略本次写出
     *
     */
    df2.write.mode("append").format("csv").save("output")
    sparkSession.sql("select*from json.`input/user.json`").show
  }

  @Test
  def load_MySQL() : Unit ={

    val properties: Properties = new Properties()
    properties.put("user","root")
    properties.put("password","123456")

    // TODO 读取
    val df: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/test", "t_job", properties)
    // 全表查询  默认show只看前20条
    df.show()

    //只查询指定的数据
    df.createTempView("jober")
    sparkSession.sql("select*from jober where job_id>10").show()


    // TODO 写
    val list = List(Person("jack", 20), Person("marry", 21))
    val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(list, 1)

    import sparkSession.implicits._

    val ds: Dataset[Person] = rdd.toDS()
    //写出的表名通常是指定一个新表
    // 写出时  DS的列名需要和写出的表的列名一致
    ds.write.mode("append").jdbc("jdbc:mysql://localhost:3306/test","t3",properties)

/*   val frame1: DataFrame = sparkSession.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "t_job")
      .load()

        frame1.write.format("jdbc")
          .option("url","jdbc:mysql://localhost:3306/test")
          .option("driver","com.mysql.jdbc.Driver")
          .option("user","root")
          .option("password","123456")
          .option("dbtable", "t_job2")
          .mode(SaveMode.Append)
          .save()*/

  }


  @After
  def stop() {

    sparkSession.close()
  }
}

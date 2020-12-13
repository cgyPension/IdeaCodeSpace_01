package com.atguigu.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}
import org.junit._

class DFAndDSTest {

   val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")
   val sparkSession: SparkSession = SparkSession.builder.config(conf).getOrCreate()


  @Test
  def testCreateSparkSession()={
    //方式1
    val session: SparkSession= SparkSession.builder
      .master("local") //提交的集群地址
      .appName("Word Count") //应用名称
      //.config("Spark.some.config.option", "some-value") //设置自定义的参数
      .getOrCreate()

    //方式2
    val conf1: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")
    val session1: SparkSession.Builder = SparkSession.builder.config(conf)
  }

  @Test
  def testCreateDF={
    val df: DataFrame = sparkSession.read.json("input/employees.json")

    //默认只显示前20行
    df.show(100)

    df.createTempView("emp")

    sparkSession.sql("select max(salary) from emp").show

    df.select("name").show

    //agg()
    df.agg(Map("salary"-> "sum")).show
  }

  /**
   *        从RDD转为DF或DS
   *         RDD   转为    DF/DS
   *
   *           scala中   ：   ①动态混入
   *                          ②隐式转换（更喜欢）
   *                             找到隐式转换的方法，在当前代码的作用域中，声明这个方法！
   */

  @Test
  def testRDDTODForDS() : Unit ={
    val list: List[Int] = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sparkSession.sparkContext.makeRDD(list, 2)

    // 导入隐式转换，这里的spark其实是环境对象的名称
    import  sparkSession.implicits._

    // todo RDD <=> DataFrame RDD <=> DataSet

    //  rdd: RDD[Int] ---->rddToDatasetHolder ---->DatasetHolder.toDS
    val ds: Dataset[Int] = rdd.toDS
    val df: DataFrame = rdd.toDF

    // todo DataFrame/DataSet <=>  RDD
    val rdd1: RDD[Row] = df.rdd
  }


  @Test
  def testDFTODS() : Unit ={
    val list: List[Person] = List(Person("jack", 20), Person("marrry", 21))
    val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(list, 2)

    import sparkSession.implicits._

    // TODO DataFrame <=> DataSet
    //RDD[Person] ----> DataSet[Row] -> DataSet[Person]
    val df: DataFrame = rdd.toDF
    val ds: Dataset[Person] = df.as[Person]

    // TODO DataSet <=> DataFrame
    val df1: DataFrame = ds.toDF()
  }

  @Test
  def testNewDF() : Unit ={
    val list = List(Person("jack", 20), Person("marry", 21))

    val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(list, 2)

    // 直接基于RDD创建   泛型必须是Product类型，Int不行,必须是对象，表结构
    val df: DataFrame = sparkSession.createDataFrame[Person](rdd)

    //  泛型必须是Product类型
    val df2: DataFrame = sparkSession.createDataFrame(list)

    //df.show()

    df2.show()
  }

  @Test
  def testCreateDF2() : Unit ={
    val list = List(Person("jack", 20), Person("marry", 21))

    val rdd: RDD[Person] = sparkSession.sparkContext.makeRDD(list, 2)

    val rdd1: RDD[Row] = rdd.map(person => Row(person.name, person.age))


    //StructType: 表结构  StructField: 一个字段的类型
    val df: DataFrame = sparkSession.createDataFrame(rdd1, StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil))
    df.show
  }


  @After
  def stop(): Unit ={
    sparkSession.close()
  }


}

// 所有样例类都继承了Product和Serializable   public class Person implements Product, Serializable
case class Person(name: String, age: Int)
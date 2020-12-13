package com.atguigu.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}
import org.junit._

class DefineFuncTest {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("My app")
  val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  // todo udf 一对一 一进一出
  @Test
  def testDefineUDF(): Unit = {
    //定义函数 年龄加1
    def fun1(salary: Int): Int = {
      salary + 1000
    }

    //注册 函数 _ 获取函数的引用
    //sparkSession.udf.register("addSalary",fun1 _)
    sparkSession.udf.register("addSalary", (x: Int) => x + 1000)

    val df: DataFrame = sparkSession.read.json("input/employees.json")

    df.createTempView("emp")
    df.show()

    sparkSession.sql("select name,addSalary(salary) from emp").show
  }


  /**
   * todo 自定义UDAF 一对多
   *    spark3.0 : 推荐使用Aggregator（强类型）
   *    spark2.0:   Aggregator 和 UserDefinedAggregateFunction (弱类型)
   *                                        容易发生类型转换异常
   *                            UserDefinedAggregateFunction: 模拟sum()
   */

  @Test
  def testUserDefinedAggregateFunction(): Unit = {
    val mysum: MySum = new MySum
    //创建函数，注册函数
    sparkSession.udf.register("mySum", mysum)

    val df: DataFrame = sparkSession.read.json("input/employees.json")

    df.createTempView("emp")
    df.show

    sparkSession.sql("select mySum(salary) from emp").show
  }

  @Test
  def testAggregator() : Unit ={
    val myavg: MyAvg = new MyAvg
      //创建函数对象，注册函数
    sparkSession.udf.register("myAvg",functions.udaf(myavg))
    val df: DataFrame = sparkSession.read.json("input/employees.json")

    df.createTempView("emp")
    df.show()

    sparkSession.sql("select myAvg(salary) from emp").show
  }

  /**
   *    在DSL中使用Aggregator
   *
   *      select myAvg(salary) from emp 在 DSL  ：  ①emp对应的应该有一个 df
   *                                                   ②  df.select(Column)
   *                                                           myAvg(salary) => Column
   *                                                           Aggregator.toColumn()
   *
   *               DS[Employee]  ： ds
   *              SQL 和 DSL的区别：输入的数据类型不同
   *              SQL:  myAvg(salary)    salary是Employee的一个属性
   *            DSL：  ds.select(Column)  列的聚合函数的输入是整个 Employee对象， DS中的 T
   *                                                     真正对T中的salary聚合是在聚合函数中自定定义
   */

    @Test
    def testAggregatorWithDSL() : Unit ={
      val df: DataFrame = sparkSession.read.json("input/employees.json")

      //import sparkSession.implicits._

      //一般直接导入implicits 在转换时会自动调用隐式方法，创建要转换类型的Encoder
      val ds: Dataset[Emp] = df.as[Emp](ExpressionEncoder[Emp]())

      //创建一个聚集函数对象
      val myavg2: MyAvg2 = new MyAvg2

      //转换为Column类型
      val result: TypedColumn[Emp, Double] = myavg2.toColumn

      //查询聚集函数，起别名
      ds.select(result.name("avgSal")).show
    }

  @After
  def stop() {
    sparkSession.close()
  }
}

case class Emp(name: String, salary: Double)

//   mySum(salary)       a(a,b,c)
class MySum extends UserDefinedAggregateFunction {

  // TODO 输入数据的结构信息
  override def inputSchema: StructType = StructType(StructField("sal", DoubleType) :: Nil)

  // TODO 缓冲区的数据结构信息
  override def bufferSchema: StructType = StructType(StructField("sal", DoubleType) :: Nil)

  // TODO 聚合函数返回的结果类型
  override def dataType: DataType = DoubleType

  // TODO 函数稳定性 是否是一个确定性（相同输入一定返回相同输出）的函数
  override def deterministic: Boolean = true

  // TODO 函数缓冲区初始化  MutableAggregationBuffer：看作Array
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0.0

  // TODO 更新缓冲区数据
  // 分区内合并： 将每一行，指定列的值，聚合到缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 从输入中取出数据和缓冲区中的数据累加，累加后将结果再放入缓冲区
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
  }


  // TODO 合并缓冲区： 将不同分区合并后的buffer再进行合并，将buffer中的值累加  将buffer2放入buffer1
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)

  }

  // TODO 返回结果 从缓冲区取出聚合后的结果
  override def evaluate(buffer: Row): Any = buffer(0)
}


/**
 * 模拟avg
 *
 *     Aggregator[-IN, BUF, OUT]: IN : 输入的类型
 *                      OUT： 输出的类型    计算逻辑需要在缓冲区保存什么样的值
 *                                                 avg:   总和  /   个数
 *                                                         (总和，个数)
 *                                                    保存的中间数据很多，可以将数据封装为一个样例类（推荐）
 *                       BUF： 缓冲区的类型
 *
 *          select avg(salary) from emp
 *
 *
 *     aggregateByKey
 *     累加器
 */
case class MyBuf(var sum: Double, var count: Int)

class MyAvg extends Aggregator[Double, MyBuf, Double] {
  //初始化缓冲区
  override def zero: MyBuf = new MyBuf(0.0,0)

  //分区内合并
  override def reduce(b: MyBuf, a: Double): MyBuf = {
    b.sum+=a
    b.count +=1
    b
  }

  //分区间合并 间b2合并到b1上
  override def merge(b1: MyBuf, b2: MyBuf): MyBuf = {
    b1.sum +=b2.sum
    b1.count +=b2.count
    b1
  }

  //返回结果
  override def finish(reduction: MyBuf): Double = reduction.sum/reduction.count

  //缓冲区的Encoder
  override def bufferEncoder: Encoder[MyBuf] = ExpressionEncoder[MyBuf]() //Encoders.product[MyBuf]

  //输出类型的Encoder
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class MyAvg2 extends Aggregator[Emp,MyBuf,Double]{
  override def zero: MyBuf = MyBuf(1.0,0)

  override def reduce(b: MyBuf, a: Emp): MyBuf = {
    b.sum +=a.salary
    b.count +=1
    b
  }

  override def merge(b1: MyBuf, b2: MyBuf): MyBuf = {
    b1.sum +=b2.sum
    b1.count +=b2.count
    b1
  }

  override def finish(reduction: MyBuf): Double = reduction.sum/reduction.count

  override def bufferEncoder: Encoder[MyBuf] = Encoders.product[MyBuf]

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}






















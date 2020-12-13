package com.atguigu.SparkCore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

import scala.collection.mutable

/**
 *    SparkCore:    RDD（使用最广泛，最基础）
 *
 *              针对Job中共享变量：
 *                     广播变量：    用来在Job中将共享的只读变量，通过广播的方式发送所有的Executor，每个Executor上执行的
 *                                task共享变量！
 *
 *                                Executor可以从广播网络中的任意节点（而不是局限于Driver）来获取广播变量！
 *
 *                                在Spark内部，会将不同阶段用到的共用的数据，以广播变量的形式进行发送！
 *
 *                                 广播变量 缓存在机器上，而不是 随着每一个Task进行传输！
 *
 *                                 场景： 每一个task，在不同的阶段需要使用共同的数据（大数据）！
 *
 *                                 使用：   var broadcastVar=SparkContext.broadcast(v)  //创建广播变量
 *                                          broadcastVar.value  //访问广播变量
 *
 *                                          .unpersist()   // 释放executor上的广播变量，释放后，如果后续使用可以重新广播
 *                                          .destroy()    //   释放executor上的广播变量，不可以再获取
 *
 *                                  一旦创建了broadcastVar，就必须使用broadcastVar而不是v！
 *                     累加器:
 *                              作用： 在Job中适用于仅仅是追加操作的场景，类似MR中的计数器Counter 或 Sum求和操作！
 *                                    spark默认只提供了针对数值类型累加的累加器，可以扩展！
 *                                    SparkContext.longAccumulator() or SparkContext.doubleAccumulator()
 *
 *                               方法：  add() : 累加
 *                                      value() : 获取累加的结果
 *
 *                                      必须实现的方法： reset() : 将累加器重置归0
 *                                                      merge() : 将其他相同类型的累加器进行合并！
 *
 *
 *                               在executor上不能读累加器的值，只能在Driver端获取累加器的值；
 *                               程序员可以通过extends AccumulatorV2实现自己的累加器！
 *
 *                               使用：  SparkContext.register(自定义累加器)
 *                               调用流程：   每个Task：
 *                                         copy()-----reset()-----isZero()-----序列化
 *
 *                                         foreach是行动算子，map是转换，foreach能触发累加器
 */
class RDD_BroadCastAndACCTest {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("My app"))

  @Before
  def start(){

    //删除output目录
    val fileSystem: FileSystem = FileSystem.get(new Configuration())

    val path = new Path("output")

    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }

  }

  @Test
  def testBroadcastVar() : Unit ={

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //有一个很大的集合  10G
    val range = Range(1, 10000)

    //使用广播变量
    val br: Broadcast[Range] = sc.broadcast(range)

    //判断当前rdd中哪些元素，在range中会出现，将出现的元素进行保存

    //range 只读
    rdd.filter(x=>br.value.contains(x))

    rdd.saveAsTextFile("output")

    // TODO 另一应用场景
    // join会有笛卡尔乘积效果，数据量会急剧增多。如果有shuffle操作，那么性能会非常低
    // TODO 为了解决join出现性能问题，可以将数据独立出来，防止shuffle操作。
    // 这样的话。会导致数据每一个task会复制一份，那么executor内存中会有大量冗余数据，性能也会受到影响。
    // 所以可以采用广播变量，将数据保存到executor的内存中。
  }


  // TODO 累加器 ： 分布式共享只写变量

  // 所谓的累加器，一般的作用为累加（数值增加，数据的累加）数据

  // 1. 将累加器变量注册到spark中
  // 2. 执行计算时。spark会将累加器发送到executor执行计算
  // 3. 计算完毕后，executor会将累加器的计算结果返回到driver端。
  // 4. driver端获取到多个累加器的结果，然后两两合并。最后得到累加器的执行结果。
  @Test
  def testACC(): Unit ={

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val sumAccumulator: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(x=>sumAccumulator.add(x))

    // Driver端声明的sum，没有变化  sum=0
    println(sumAccumulator.value)
  }


  @Test
  def testACC2(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val intAccumulator = new MyIntAccumulator()

    //注册
    sc.register(intAccumulator,"MySum")

    rdd.foreach(x=>intAccumulator.add(x))

    // Driver端声明的sum，没有变化  sum=0
    println(intAccumulator.value)

  }



   //没有调用reduceByKey使用累加器实现的累加，省略了shuffle! 提高了效率！

  @Test
  def testWordCountAccumulator (): Unit = {

    val wcAcc = new WordCountAccumulator

    sc.register (wcAcc, "wc")

    val rdd: RDD[String] = sc.textFile ("input/hello1.txt")

    val rdd1: RDD[String] = rdd.flatMap (line => line.split (" ") )

    rdd1.foreach (word => wcAcc.add (word) )

    println (wcAcc.value)
  }

    @After
  def stop(){
    sc.stop()
  }
}

// TODO 自定义累加器
//  1. 继承 AccumulatorV2，定义泛型 [IN, OUT]
//         IN   : 累加器输入值的类型
//         OUT : 累加器返回结果的类型
//  2. 重写方法(6)
//  3. copyAndReset must return a zero value copy
class MyIntAccumulator(num:Int=10) extends AccumulatorV2[Int,Int]{

  //用來保存累加的值
  var result = num

  //是否归0
  override def isZero: Boolean = result==0  //false

  //返回当前累加器的一个副本
  override def copy(): AccumulatorV2[Int, Int] = new MyIntAccumulator()

  //重置累加器归0
  override def reset() = result=0

  //累加
  override def add(v: Int): Unit = result+=v

  //合并相同类型的累加器的值
  override def merge(other: AccumulatorV2[Int, Int]): Unit = result+=other.value

  //获取累加的值
  override def value:Int = result
}



//自定义累加器实现WordCount,对单词进行累加  输入：String  输出：Map[String,Int]
class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

  private val result: mutable.Map[String, Int] = mutable.Map[String, Int]()

  override def isZero: Boolean = result.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new WordCountAccumulator()

  override def reset(): Unit = result.clear() // result=immutable.Map[String, Int]()

  override def add(v: String): Unit = {

    result.put(v, result.getOrElse(v, 0) + 1)

  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

    val toMergeMap: mutable.Map[String, Int] = other.value

    for ((word, count) <- toMergeMap) {
      result.put(word, result.getOrElse(word, 0) + count)
    }

  }

  override def value: mutable.Map[String, Int] = result
}





























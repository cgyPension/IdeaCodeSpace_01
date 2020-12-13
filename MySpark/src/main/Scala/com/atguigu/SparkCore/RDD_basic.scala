package com.atguigu.SparkCore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._



class RDD_basic {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("My app"))


  // TODO saveAsTextFile方法如果文件路径已经存在，会发生错误
  @Before
  def Start(): Unit = {
    //删除output目录
    val fileSystem = FileSystem.get(new Configuration())
    val path = new Path("output")
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
  }


  // TODO Spark - 从内存中创建RDD
  @Test
  def rdd_memory(): Unit = {
    val list = List(1, 2, 3, 5)
    //1.parallelize：并行
    val rdd1: RDD[Int] = sc.parallelize(list)

    //等价 makeRDD的底层代码其实就是调用了parallelize方法
    val rdd2: RDD[Int] = sc.makeRDD(list)

    rdd1.saveAsTextFile("output")

  }


  // TODO  Spark - 从磁盘（File）中创建RDD
  // path : 读取文件（目录）的路径
  // path可以设定相对路径，如果是IDEA，那么相对路径的位置从项目的根开始查找。
  // path路径根据环境的不同自动发生改变。
  // Spark读取文件时，默认采用的是Hadoop读取文件的规则
  // 默认是一行一行的读取文件内容
  // 如果路径指向的为文件目录。那么这个目录中的文本文件都会被读取
  @Test
  def rdd_file(): Unit ={
    val fileRDD:RDD[String] = sc.textFile("input")
    // 文件路径可以采用通配符
    //val fileRDD:RDD[String] = sc.textFile("input/word*.txt")
    // 文件路径还可以指向第三方存储系统：HDFS
    //val fileRDD: RDD[String] = sc.textFile("input/word*.txt")

    println(fileRDD.collect().mkString(","))
  }

  // TODO
  //    RDD中的分区的数量就是并行度，设定并行度，其实就在设定分区数量
  // 1. makeRDD的第一个参数：数据源
  // 2. makeRDD的第二个参数：默认并行度(分区的数量)
  // TODO numSlices: Int = defaultParallelism（默认并行度）
  /**
   *    scheduler.conf.getInt("spark.default.parallelism", totalCores)
   *    standalone / YARN模式， totalCores是Job申请的总的核数！
   *   并行度默认会从spark配置信息中获取spark.default.parallelism值。
   *    本地提交时如果获取不到指定参数，会采用默认值totalCores（机器的总核数）
   *   机器总核数 = 当前环境中可用核数
   *    local => 单核（单线程）=> 1
   *    local[4] => 4核（4个线程） => 4
   *    local[*] => 最大核数 => 8
   */
  @Test
  def rdd_memory_par(): Unit = {
    println(new SparkConf().getInt("spark.default.parallelism", -1))
  }

  // TODO 内存中的集合数据按照平均分的方式进行分区处理
  @Test
  def rdd_memory_partitiondata(): Unit = {
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    //12,34
    //rdd.saveAsTextFile("output")

    val rdd1 = sc.makeRDD(List(1,2,3,4), 4)
    // 1,2,3,4
    //rdd1.saveAsTextFile("output1")

    // TODO 内存中的集合数据如果不能平均分，会将多余的数据放置在最后一个分区。
    // 内存中数据的分区基本上就是平均分，如果不能整除，会采用一个基本的算法实现分配
    // List(1,2,3,4,5) = Array(1,2,3,4,5)
    // (length = 5, num = 3)
    // (0,1,2)
    // => 0 => (0, 1) => 1
    // => 1 => (1, 3) => 2,3
    // => 2 => (3, 5) => 4,5
    // Array.slice => 切分数组 => (from , until)

    val arr = Array(1, 2, 3, 5)
    println(arr.slice(0, 2).mkString(","))

    val rdd2 = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    // 1，23，45
    //rdd2.saveAsTextFile("output")

  }


  // textFile 第一个参数表示读取文件的路径
  // textFile 第二个参数表示最小分区数量
  //          默认值为： math.min(defaultParallelism, 2)
  //                     math.min(8, 2) => 2
  // TODO 1. Spark读取文件采用的是Hadoop的读取规则

  //    数据读取规则 ： 以行为单位来读取
  //    文件切片规则 :  以字节方式来切片


  // 2. 问题
  //    TODO 文件到底切成几片（分区的数量）?
  //    文件字节数（10），预计切片数量（2）
  //    10 / 2 => 5byte
  // totalSize = 10
  // goalSize(期望每片大小) = totalSize / numSplits = 10 / 2 = 5 ...1 => 3
  //  要是11就会产生三个分区5、5、1
  //   所谓的最小分区数，取决于总的字节数是否能整除分区数（goalSize(分区大小)）并且剩余的字节达到一个比率(1.1(hadoop块大小的比率))，不断循环切片，对比比率
  // TODO 结合实际128m块大小去想，避免产生128.1，分成128、0.1这种小文件问题

  //   实际产生的分区数量可能大于最小分区数

  //   TODO 分区的数据如何存储？

  /**
   * 分区数据是以行为单位读取的是，而不是字节，并不会把行给切了，比如1234567890，分两个分区，
   *  按道理应该是12345,67890各一个分区，但是这里会把一行数据直接放在第一个分区
   */


  @Test
  def rdd_file_partitiondata(): Unit = {
    // TODO 1. 分几个区？
    //    10 byte / 4 = 2byte...2byte => 5   //运算时转为int类型除不尽，省略小数位
    //    0 => (0, 2)
    //    1 => (2, 4)
    //    2 => (4, 6)
    //    3 => (6, 8)
    //    4 => (8,10)

    // TODO 2. 数据如何存储?
    //    数据是以行的方式读取，但是会考虑偏移量（数据的offset）的设置
    //    1@@ => 012
    //    2@@ => 345
    //    3@@ => 678
    //    4   => 9

    //    0 => (0, 2) => 1
    //    1 => (2, 4) => 2
    //    2 => (4, 6) => 3
    //    3 => (6, 8) =>
    //    4 => (8,10) => 4

    // 12,34
    //val fileRDD1: RDD[String] = sc.textFile("input/w.txt")
    //fileRDD1.saveAsTextFile("output1")

    //val fileRDD2: RDD[String] = sc.textFile("input/w.txt",1)
    //fileRDD2.saveAsTextFile("output2")

    // X
    val fileRDD3: RDD[String] = sc.textFile("input/w.txt",4)
    fileRDD3.saveAsTextFile("output3")

    // X
    val fileRDD4: RDD[String] = sc.textFile("input/w.txt",3)
    fileRDD4.saveAsTextFile("output4")

    /**
     * totalsize = 6, num = 2
     * 6/2=3byte
     * (0,0+3) => (0,3)
     * (3,3+3) => (3,6)
     * 是包含末尾3的
     * 1@@ => 012
     * 234 => 345
     * 所以结果都会在0号分区
     */


    // TODO hadoop分区是以文件为单位进行划分的。
    //      读取数据不能跨越文件

    // 1@@ => 012
    // 234 => 345

    // 12 / 3 = 4
    // 1.txt => (0, 4)
    //       => (4, 6)
    // 2.txt => (0, 4)
    //       => (4, 6)

    val fileRDD5: RDD[String] = sc.textFile("input", 3)
    fileRDD5.saveAsTextFile("output5")

  }


  @After
  def stop(): Unit = {
    sc.stop()
  }


}

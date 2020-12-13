package com.atguigu.SparkCore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

/**
 *    Cache缓存：  缓存为了提高对重复元素的查询效率
 *
 *                为了提高查询效率，多使用内存作为缓存设备！
 *                    优秀的缓存： Ehcache, MemCache , Redis
 *
 *                在Spark中可以将RDD进行缓存！ 对于重复计算的步骤，可以直接从缓存中取出已经计算好的RDD！
 *
 *                缓存都会有数据的淘汰策略：
 *                      LRU（less recent use）: 优先淘汰最近最少使用的数据
 *                      TTL ： 可以在每个数据写入缓存时，指定数据的过期时间，优先淘汰快要过期的数据，ttl()返回值小
 *                      Random : 随机淘汰
 *
 *                 在Spark中，使用 LRU策略！
 *
 *                 在Spark中，可以通过 persist(StorageLevel)来指定缓存的设备！ 优先推荐使用内存作为缓存！
 *
 *                 缓存不会改变RDD的血缘的关系！ 原因在于缓存多使用内存，因此不可靠，在容错时，如果缓存中没有可以使用的RDD，就
 *                 需要根据血缘关系重新计算！
 *
 *
 *        Mybatis: ORM(object relational mapping)框架
 *                  ORM： 对象关系映射
 *
 *                  将一个Java中的对象， 通过方法，将对象中的数据写入到关系(数据库中的表)中
 *                     Java： new Person("Tom","male",20)
 *                     Mysql :   表中的一条记录
 *
 *                 Java用来作为后端语言，开发后端应用！
 *
 *                 通过html页面----->点击按钮----->向后端应用发送http请求----->处理请求
 *                      ----->验证页面表单中填写的数据是否正确
 *                      ----->  将http请求中携带的用户名，密码参数 ---->封装的JavaBean(数据模型)
 *                      ------>  调用方法    validUser(Person("Tom","male",20) )
 *                      ------>  从数据库中，根据Person中的属性进行查询，将查询出的记录 封装为
 *                                Person("Tom","male",30)
 *                              将两个Person对象，进行比较，确定用户传入的参数是否正确！
 *
 *                 从数据库  查询
 *                 写入数据库
 *
 *
 *     checkpoint:  检查点。
 *                  因为缓存的不可靠，所以Spark提供了对RDD的另外一种持久化方式，允许将RDD通过ck持久化到文件系统中！
 *
 *                  SparkContext#setCheckpointDir: 设置ck目录,如果是集群模式，必须是HDFS上的路径！
 *
 *                  // TODO 检查点操作会切断血缘关系。一旦数据丢失不会重头读取数据
 *                 // 因为检查点会将数据保存到分布式存储系统中，数据相对来说比较安全。不容易丢失。
 *                // 所以会切断血缘，等同于产生新的数据源。不会对容错机制产生影响！
 *
 *                  checkpoint 会在 RDD的第一个行动算子执行时执行！
 *
 *                  在使用checkpoint时，结合使用cache，避免checkpoint的Job重复计算！
 *
 *
 *    persist: 将计算结果进行缓存，重复使用，提高效率
 *             默认的缓存是存储在Executor端的内存中,数据量大的时候，该如何处理？
 *             TODO 缓存cache底层其实调用的persist方法
 *             persist方法在持久化数据时会采用不同的存储级别对数据进行持久化操作
 *             cache缓存的默认操作就是将数据保存到内存
 *             cache存储的数据在内存中，如果内存不够用，executor可以将内存的数据进行整理，然后可以丢弃数据。
 *             如果由于executor端整理内存导致缓存的数据丢失，那么数据操作依然要重头执行。
 *             如果cache后的数据重头执行数据操作的话，那么必须要遵循血缘关系，所以cache操作不能删除血缘关系。
 *
 *
 *              val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
 *              sparkConf.set("spark.local.dir.blockmgr", "D:\\mineworkspace\\idea\\classes\\classes-0213\\input")
 *              val sc = new SparkContext(sparkConf)
 *
 */


object RDD_Cache {

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
  def testCache1() : Unit ={
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd2 = rdd.map(x => {
      println("map")
      x
    })

    println("-------------缓存之前的血缘--------------")
    println(rdd2.toDebugString)

    //将rdd2进行缓存
    rdd2.cache()

    println("-------------缓存之后的血缘--------------")
    println(rdd2.toDebugString)

    // cache() 等价于  persist()  等价于 persist(StorageLevel.MEMORY_ONLY)
    println(rdd2.collect().mkString(","))

    println("-------------------------------------------")

    rdd2.saveAsTextFile("output")

  }

  @Test
  def testCache2() : Unit ={
    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd2 = rdd.map(x => {
      println("map")
      (x,1)
    })

    // 当我们使用了会产生shuffle的算子时，shuffle阶段MapTask的结果会自动缓存(本地磁盘)！
    val rdd3: RDD[(Int, Int)] = rdd2.reduceByKey(_ + _)
    println(rdd3.collect().mkString(","))

    println("--------------------------------------")

    // 直接读取shuffle写出的文件数据，跳过shuffle的Map阶段
    rdd3.saveAsTextFile("output")
  }


  @Test
  def testCheckpoint() : Unit ={

    // TODO 将比较耗时，比较重要的数据一般会保存到分布式文件系统中。
    //      检查点的操作中为了保证数据的准确性，会执行时，会启动新的job
    //      为了提高性能，检查点操作一般会和cache联合使用
    sc.setCheckpointDir("ck")

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(list, 2)

    val rdd3: RDD[Int] = rdd.map(x => x).map(x => x).map(x => x).map(x => x).map(x => x).map(x => x).map(x => x)

    val rdd2 = rdd3.map(x => {
      println("map")
      x
    })

    println("-------------ck之前的血缘--------------")

    println(rdd2.toDebugString)

    // 和 cache的不同，类似行动算子(额外提交一个Job)，真正运行也是需要第一个行动算子触发
    rdd2.checkpoint()

    //将rdd2缓存进内存，避免checkpoint（不是避免提交Job）,提交Job后，避免重复计算
    rdd2.cache()

    //发现当前rdd2需要checkpoint，会在collect后，将rdd2的数据写入到存储设备中
    println(rdd2.collect().mkString(","))

    println("-------------ck之后的血缘--------------")
    println(rdd2.toDebugString)

    println("--------------------------------------")

    //从ck目录中取rdd2
    rdd2.saveAsTextFile("output")
  }











  @After
  def stop(){
    sc.stop()
  }

}

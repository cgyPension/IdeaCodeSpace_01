package com.atguigu.SparkCore


import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.junit._

import scala.reflect.ClassTag

class RDD_operator_transfer {

  val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("RDD_operator"))


  // TODO Spark - RDD - 算子（方法）
  @Test
  def rdd_operator1(): Unit = {

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 转换算子能够将旧的RDD通过方法转换为新的RDD，但是不会触发作业的执行
    // 转换：旧RDD => 算子 => 新RDD

    // TODO 分区问题
    // RDD中有分区列表
    // 默认分区数量不变，数据会转换后输出
    val rdd1: RDD[Int] = rdd.map(_ * 2)
    // 读取数据
    // collect方法不会转换RDD，会触发作业的执行
    // 所以将collectt这样的方法称之为行动（action）算子
    val ints: Array[Int] = rdd1.collect()

    rdd1.saveAsTextFile("output")
  }


  @Test
  def rdd_operator2(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 1,2,3,4 => x map 1 => x map 2
    // 1 => x map 1  => x map 2  => 2

    // 0 - (1,2)         1A       1B   2A  2B
    // 1 - (3,4)  3A  3B    4A  4B
    // TODO 分区内数据是按照顺序依次执行，第一条数据所有的逻辑全部执行完毕后才会执行下一条数据
    //      分区间数据执行没有顺序，而且无需等待

    val rdd1: RDD[Int] = rdd.map(x => {
      println("map A = " + x)
      x
    })

    val rdd2: RDD[Int] = rdd1.map(x => {
      println("map B = " + x)
      x
    })

    println(rdd2.collect().mkString(","))

  }


  @Test
  def testmap(): Unit = {
    val list = List(1, 2, 3, 5, 6)
    val rdd1: RDD[Int] = sc.makeRDD(list, 2)

    //在Driver
    var i = 1;

    // Connection

    //map在executor端运行
    //map算子，不会改变之前RDD的分区数，也不会改变元素的分区！
    //map的特点是1对1
    val value: RDD[Int] = rdd1.map(_ + i)
    //value.saveAsTextFile("output")

    val value2: RDD[Int] = rdd1.map(x => {
      //Connection
      //new Connection(每次)
      x + i
    })

    /*   val value3: RDD[Int] = rdd1.mapPartitions(x => {
         // Connection
         // new  Connection(一次)
         x + i
       })*/


    /**
     * 小功能：从服务器日志数据apache.log中获取用户请求URL资源路径
     */
    val rdd2 = sc.textFile("input/apache.log", 1)
    val result = rdd2.map(line => line.split(" ")(6))
    result.collect().foreach(println)
  }


  @Test
  def testMapPartitions(): Unit = {
    // 以分区为单位进行计算， 和map算子很像
    // 区别就在于map算子是一个一个执行，而mapPartitions一个分区一个分区执行
    // 类似于批处理

    // map方法是全量数据操作，不能丢失数据
    // mapPartitions 一次性获取分区的所有数据，那么可以执行迭代器集合的所有操作
    //               过滤，max,sum
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 3,4,1,2
    //        val rdd2: RDD[Int] = rdd1.mapPartitions(
    //            iter => {
    //                iter.map(_*2)
    //            }
    //        )
    //        println(rdd2.collect.mkString(","))

    val rdd2: RDD[Int] = rdd1.mapPartitions(
      iter => {
        iter.filter(_ % 2 == 0)
      }
    )
    println(rdd2.collect.mkString(","))

    //小功能：获取每个数据分区的最大值,迭代器类型要返回迭代器
    val result2: RDD[Int] = rdd1.mapPartitions(x => List(x.max).iterator)
    //result2.saveAsTextFile("output")

    // 获取每个分区最大值以及分区号
    val result3: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index, iter) => {
      List((index, iter.max)).iterator
    })
    println(result3.collect.mkString(","))


    //获取第二个数据分区的数据
    val result4: RDD[Int] = rdd1.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        //不符合要求，返回空集合迭代器
        Nil.iterator
      }

    })
    println(result4.collect.mkString(","))

  }


  @Test
  def testflatMap(): Unit = {
    val list = List(List(1, 2), 3, List(4, 5))
    val rdd: RDD[Any] = sc.makeRDD(list, 2)
    val result: RDD[Int] = rdd.flatMap(x => {
      x match {
        case a: List[Int] => a
        case b: Int => List(b)
      }
    })

    result.saveAsTextFile("output")
  }


  @Test
  def testglom(): Unit = {

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // todo glom => 将每个分区的数据转换为数组
    val rdd: RDD[Array[Int]] = dataRDD.glom()

    rdd.foreach(
      array => {
        println(array.mkString(","))
      }
    )

    /**
     * 	小功能：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
     */
    val data1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // 将每个分区的数据转换为数组
    val glomRDD: RDD[Array[Int]] = data1.glom()

    // 将数组中的最大值取出
    // Array => max
    val maxRDD: RDD[Int] = glomRDD.map(array => array.max)

    // 将取出的最大值求和
    val array: Array[Int] = maxRDD.collect()

    //sum()也是一个行动算子    count(),avg(),max(),min()
    println(array.sum)

  }


  /*
    TODO 只要看到算子，需要传入一个 Partitioner 或 numPartitions，此算子一定会产生shuffle!
     shuffle类型的算子，会创建ShuffledRDD
  */
  @Test
  def testgroupBy(): Unit = {
    // TODO 分组
    // groupBy 方法可以根据指定的规则进行分组，指定的规则的返回值就是分组的key
    // groupBy 方法的返回值为元组
    //      元组中的第一个元素，表示分组的key
    //      元组中的第二个元素，表示相同key的数据形成的可迭代的集合
    // groupBy方法执行完毕后，会将数据进行分组操作，但是分区是不会改变的。
    //      不同的组的数据会打乱在不同的分区中
    // groupBy方法方法会导致数据不均匀，产生shuffle操作。如果想改变分区，可以传递参数。

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    var rdd = dataRDD.groupBy(num => {
      num % 2
    })

    //产生两个分区
    /*var rdd = dataRDD.groupBy(num=>{
      num%2},2
    )*/

    rdd.saveAsTextFile("output")

    //glom => 分区转换为一个array
    println(" 分组后的数据分区的数量 = " + rdd.glom().collect().length)

    rdd.collect().foreach {
      case (key, list) => {
        println("key:" + key + " list【" + list.mkString(",") + "】")
      }
    }

    //	小功能：将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组
    val test1 = sc.makeRDD(List("Hello", "hive", "hbase", "Hadoop"), 2)
    test1.groupBy(word => {
      //word.substring(0,1)
      //word.charAt(0)
      //String(0)=>StringOps
      //隐式转化
      word(0)
    })

    //	小功能：从服务器日志数据apache.log中获取每个时间段访问量。
    val test2: RDD[String] = sc.textFile("input/apache.log",1)

    val timeRDD: RDD[(String, Iterable[String])] = test2.groupBy(line => line.split(" ")(3).split(":")(1))

    val result: RDD[(String, Int)] = timeRDD.map {
      case (time, iter) => (time, iter.size)
    }

    result.saveAsTextFile("output")


    //	小功能：WordCount。
    val test3 = sc.makeRDD(List("Hello Scala", "Hello"))
    println(test3.flatMap(_.split(" "))
      .groupBy(word => word)
      .map(kv => (kv._1, kv._2.size))
      .collect().mkString(","))
  }


  @Test
  def testfilter(): Unit = {
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // TODO filter过滤
    // 根据指定的规则对数据进行筛选过滤，满足条件的数据保留，不满足的数据丢弃
    val rdd: RDD[Int] = dataRDD.filter(
      num => {
        num % 2 == 0
      }
    )

    rdd.collect().foreach(println)

    //	小功能：从服务器日志数据apache.log中获取2015年5月17日的请求路径
    val test1: RDD[String] = sc.textFile("input/apache.log")
    val timeRDD = test1.map(log => {
      val datas = log.split(" ")
      datas(3)
    })

    timeRDD.filter(time => {
      val ymd = time.substring(0, 10)
      ymd == "17/05/2015"
    }).collect().foreach(println(_))
  }


  // TODO sample用于从数据集中抽取数据
  // 第一个参数表示数据抽取后是否放回，可以重复抽取
  //    true : 抽取后放回
  //    false: 抽取不放回
  // 第二个参数表示数据抽取的几率（不放回的场合）， 重复抽取的次数（放回的场合）
  //    这里的几率不是数据能够被抽取的数据总量的比率。
  // 第三个参数表示随机数的种子,可以确定数据的抽取
  //    随机数不随机，所谓的随机数依靠随机算法实现
  @Test
  def testsample(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val rdd1: RDD[Int] = rdd.sample(false, 0.5, 1)
    val rdd2: RDD[Int] = rdd.sample(true, 2, 1)
    val rdd3: RDD[Int] = rdd.sample(false, 0.7, 1)
    val rdd4: RDD[Int] = rdd.sample(false, 0.7)
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    println(rdd3.collect().mkString(","))
    println(rdd4.collect().mkString(","))
  }


  @Test
  def testdistinct(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 1, 2, 4))

    //distinct是可能会产生shuffle的算子！
    // distinct去重的原理就是对key进行分组，分组后去重！
    //rdd.groupBy(num => num,2)

    /**
     *  如果当前RDD有分区器，且分区的数量和 distinct(numPartions)相同，就不会产生shuffle!
     * 什么样的RDD有分区器： ①K-V类型的RDD会有分区器
     *                       ②执行了类似 corgrop这种操作时，下游RDD会自动生成分区器
     *
     *               如果RDD已经有了分区器，那么代表RDD已经使用所带的分区器对数据进行了分区！
     *               			key相同的已经在一个区内，不需要再分组，没有shuffle！
     *
     *               去重为什么会有shuffle： 去重是利用分区去重，需要先将key相同的数据，分到一组！
     *               					分组时产生了shuffle！
     */

    // TODO distinct 去重
    val rdd1: RDD[Int] = rdd.distinct()
    //distinct可以改变分区的数量
    val rdd2: RDD[Int] = rdd.distinct(2)

    println(rdd2.partitioner)

    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))


  }


  @Test
  def testcoalesce(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 2), 6)
    // [1,1,1], [2,2,2]
    // [],[2,2,2]
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 == 0)

    //多 => 少
    // todo 当数据过滤后，发现先数据不够均匀，那么可以缩减分区,缩减到几个分区
    // val coalesceRDD= filterRDD.coalesce(1)
    //coalesceRDD.saveAsTextFile("output")

    // todo 如果返现数据分区不合理，也可以缩减分区
    val coalesceRDD = rdd.coalesce(2)
    coalesceRDD.saveAsTextFile("output")


    // TODO 扩大分区
    // coalesce主要目的是缩减分区，扩大分区时没有效果
    // 为什么不能扩大分区，因为在分区缩减时，数据不会打乱重写组合,没有shuffle的过程

    // 如果就是非得想要将数据扩大分区，那么必须打乱数据后重新组合,必须使用shuffle

    // TODO coalesce方法第一个参数表示缩减分区后的分区数量
    //      coalesce方法第二个参数表示分区改变时，是否会打乱重新组合数据，默认不打乱
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 2), 2)
    val value = rdd2.coalesce(6, true)
    value.saveAsTextFile("output")
  }


  @Test
  def testrepartition(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 2), 3)
    // TODO 扩大分区 ： repartition
    // 从底层源码的角度。repartition其实就是coalesce，并且肯定进行shuffle操作
    rdd.repartition(6)
  }

  @Test
  def testsortBy(): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 2, 3))
    // TODO sortBy

    // 有shuffle 默认排序规则为 升序 不是Hash分区，使用RangePartitioner分区！
    // sortBy可以通过传递第二个参数改变排序的方式
    // sortBy可以设定第三个参数改变分区。
    val sortRDD: RDD[Int] = rdd.sortBy(num => num, false)
    println(sortRDD.collect().mkString(","))



  /*  如何实现排序：  无比较，不排序！
    比较： compareTo()

    Ordered:  需要让自定义的类，混入Ordered，在类中实现compareTo()

    将Ordered类型的类，称为可以排序的类！

    Ordering：  比较器。可以使用排序器对针对类型进行排序
    使用Ordering, 自定义的类不需要混入Ordered特质。
    在Ordering提供针对排序类的compareTo()

    更灵活*/
    val list = List(Person("marry", 20), Person("jack", 20), Person("jack", 21), Person("tom", 30))
    val rdd2: RDD[Person] = sc.makeRDD(list, 2)

    //要求先按照名称降序排序，之后名称相同的按照年龄降序排序    K=>Tuple2
    // val result: RDD[Person] = rdd.sortBy(person => (person.name, person.age), false,1)

    //要求先按照名称降序降序，之后名称相同的按照年龄升序排序    K=>Tuple2
    val result: RDD[Person] = rdd2.sortBy(person => (person.name, person.age),numPartitions = 1)(Ordering.Tuple2(Ordering.String.reverse, Ordering.Int), ClassTag(classOf[Tuple2[String, Int]]))

    result.saveAsTextFile("output")


    /*******************************************/

    //如果自定义key进行排序，需要将key混入特质ordered
    /*    class User extends Ordered[User] with Serializable{
          class User extends Serializable {
              override def compare(that:User):Int = {
                1
              }
          }
        }*/

    val list2 = List(Person1("marry", 20), Person1("jack", 20), Person1("jack", 21), Person1("tom", 30))

    val rdd3: RDD[Person1] = sc.makeRDD(list2, 2)

    val result2: RDD[Person1] = rdd3.sortBy(p => p,numPartitions = 1)

    result2.saveAsTextFile("output")


  }

  @Test
  def testSortByKey2() : Unit ={

    val list = List(Person("jack", 20), Person("jack", 21), Person("marry", 21), Person("tom", 23))

    val rdd: RDD[Person] = sc.makeRDD(list, 2)

    // 按照年龄排序,就将这个数据作为key
    /* val rdd2: RDD[(Int, Person)] = rdd
       .map(person => (person.age, person))

     val result: RDD[(Int, Person)] = rdd2.sortByKey(false, 1)*/


    // 只能排序全部升序或降序
    /* val rdd2: RDD[((Int, String), Person)] = rdd.map(person => ((person.age, person.name), person))

     val result: RDD[((Int, String), Person)] = rdd2.sortByKey(false, 1)*/

    // 先按照年龄排序，年龄降序排序，年龄相同的，继续按照名称升序排序
    // 提供一个隐式的针对Person类型排序的Ordering类型的比较器

    implicit val ord=new Ordering[Person](){
      // 先按照年龄排序，年龄降序排序，年龄相同的，继续按照名称升序排序
      override def compare(x: Person, y: Person): Int = {

        var result: Int = -x.age.compareTo(y.age)
        if (result == 0){
          result=x.name.compareTo(y.name)
        }
        result
      }
    }

    val rdd1: RDD[(Person, Int)] = rdd.map(person => (person, 1))

    // 年龄升序，名称降序
    val result: RDD[(Person, Int)] = rdd1.sortByKey(false,numPartitions = 1)

    result.saveAsTextFile("output")

  }



  // TODO 各种集
  // subtract,union,intersection 要求两个RDD数据类型必须一致！
  @Test
  def testji(): Unit = {

    val rdd0: RDD[String] = sc.makeRDD(List("a", "b", "c", "d"), 2)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    // ( Int, String )
    //val value: RDD[(Int, String)] = rdd1.zip(rdd0)

    // 1，2，3，4
    // 3，4，5，6

    // TODO 并集, 数据合并，分区也会合并
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    println(rdd3.collect().mkString(","))
    //println(rdd3.partitioner)
    //rdd3.saveAsTextFile("output3")

    // TODO 交集 ： 保留最大分区数 ，数据被打乱重组, shuffle
    // 有shuffle,结果默认使用HashPartitioner，分上游RDD最大分区数！
    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd4.collect().mkString(","))
    //println(rdd4.partitioner)
    //rdd4.saveAsTextFile("output4")

    // TODO 差集 : 数据被打乱重组, shuffle
    // 当调用rdd的subtract方法时，以当前rdd的分区为主，所以分区数量等于当前rdd的分区数量
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))
    //println(rdd5.partitioner)
    //rdd5.saveAsTextFile("output5")

    // TODO cartesian:笛卡尔积 没有shuffle
    rdd1.cartesian(rdd2).saveAsTextFile("output")


    // TODO 拉链 : 分区数不变
    // TODO 2个RDD的分区一致,但是数据量相同的场合:
    //   Exception: Can only zip RDDs with same number of elements in each partition
    // TODO 2个RDD的分区不一致，数据量也不相同，但是每个分区数据量一致：
    //   Exception：Can't zip RDDs with unequal numbers of partitions: List(3, 2)
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    rdd6.saveAsTextFile("output6")

    // TODO zipAll
    val list1 = List(1, 2, 3, 4)
    val list2 = List("a", "b", "c","d","e","f")
    println(list1.zipAll(list2, 5, "d"))


    // TODO ZipWithIndex  当前元素和索引进行zip操作。 索引是在集合中的索引而不是在分区中的索引！
    println(list1.zipWithIndex)

    rdd1.zipWithIndex().saveAsTextFile("output")

    // TODO zipPartitions  可以返回任意类型的结果，而不是局限于Tuple2
    val list3 = List(1, 2, 3, 4,5,6,7,8)
    val list4 = List("a", "b", "c","d","e","f")

    val rdd7: RDD[Int] = sc.makeRDD(list3, 2)
    val rdd8: RDD[String] = sc.makeRDD(list4, 2)

    val value: RDD[Int] = rdd7.zipPartitions(rdd8)((iter1, iter2) => iter1)
    val value2: RDD[(Int, String)] = rdd7.zipPartitions(rdd8)((iter1, iter2) => iter1.zipAll(iter2, 0, "null"))

    value2.saveAsTextFile("output")
  }


  // TODO K-V类型的数据操作
  @Test
  def testpartitionBy(): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 2)), 1)

    // TODO Spark中很多的方法是基于Key进行操作，所以数据格式应该为键值对（对偶元组）
    // 如果数据类型为K-V类型，那么Spark会给RDD自动补充很多新的功能（扩展）
    // 隐式转换(A => B)
    // (Person("jack",20)  rdd2.map(p => (p, 1))
    // 将 RDD[(Int, Int)]  MapPartitionsRDD 通过隐式转换 rddToPairRDDFunctions  转换为 PairRDDFunctions

    // TODO partitionBy : 根据指定的规则对数据进行分区
    //      groupBy
    //      filter => coalesce
    //      repartition => shuffle
    //
    // partitionBy参数为分区器对象
    //    分区器对象 ： HashPartitioner & RangePartitioner

    // HashPartitioner分区规则是将当前数据的key进行取余操作。
    // TODO HashPartitioner是spark默认的分区器
    val rdd1: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(2))
    rdd1.saveAsTextFile("output")

    //sortBy 使用了RangePartitioner
    //rdd1.sortBy()
  }

  // TODO 自定义分区器 - 自己决定数据放置在哪个分区做处理
  @Test
  def testPartitioner(): Unit = {
    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(
        ("cba", "消息1"), ("cba", "消息2"), ("cba", "消息3"),
        ("nba", "消息4"), ("wnba", "消息5"), ("nba", "消息6")
      ), 1)

    val rdd1 = rdd.partitionBy(new MyPartitioner(3))

    val rdd2 = rdd1.mapPartitionsWithIndex((index, datas) => {
      datas.map(data => (index, data))
    })
    rdd2.collect().foreach(println(_))

    //两次HashPartitioner 结果一样
    /*val rdd1: RDD[(String, String)] = rdd.partitionBy(new HashPartitioner(3))
    val rdd2: RDD[(String, String)] = rdd1.partitionBy(new HashPartitioner(3))*/

  }

    // todo  将k-v rdd中的v经过函数运算为 U，之后返回K-U
  @Test
  def testMapValues() : Unit ={
    val list = List((1, 1), (2, 1), (3, 1), (4, 1))
    val rdd: RDD[(Int, Int)] = sc.makeRDD(list, 2)
    val result: RDD[(Int, Int)] = rdd.mapValues(v => v + 1)
    result.saveAsTextFile("output")
  }

  // TODO reduceByKey : 根据数据的key进行分组，然后对value进行聚合
  @Test
  def testreduceByKey(): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello", 1), ("scala", 1), ("hello", 1)))

    // word => (word,1)
    // reduceBykey 第一个参数表示相同key的value的聚合方式
    // reduceBykey 第二个参数表示聚合后的分区数量
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey(_ + _, 2)

    println(rdd1.collect().mkString(","))
  }

  @Test
  def testgroupByKey(): Unit = {
    // TODO groupByKey : 根据数据的key进行分组
    // groupBy : 根据指定的规则对数据进行分组

    // TODO 调用groupByKey后，返回数据的类型为元组
    //   元组的第一个元素表示的是用于分组的key
    //   元组的第二个元组表示的是分组后，相同key的value的集合

    /**
     *  reduceByKey和groupByKey的区别？
     *   ①reduceByKey可以对value进行合并
     *     groupByKey只分组不合并
     *   ②reduceByKey可以在map端进行局部合并
     *      groupByKey在map端没有局部合并
     */


    val rdd = sc.makeRDD(
      List(
        ("hello", 4), ("scala",1), ("hello",1)
      )
    )
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    val wordToCount: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => {
        (word, iter.sum)
      }
    }

    println(wordToCount.collect().mkString(","))

  }

  @Test
  def testaggregateByKey(): Unit = {
    // TODO 将分区内相同key取最大值，分区间相同的key求和

    // 0 => 【 (a,2), (c,3) 】
    //                         => 【 (a,2)，(b,4)，(c,9) 】
    // 1 => 【 (b,4), (c,6)】

    // TODO 分区内和分区间计算规则不一样。
    // reduceByKey : 分区内和分区间计算规则相同

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("c", 3),
        ("b", 4), ("c", 5), ("c", 6)
      )
      , 2
    )

    // TODO aggregateByKey ： 根据key进行数据聚合
    // Scala 语法 ： 函数柯里化
    // 方法有两个参数列表需要传递参数
    // 第一个参数列表中传递参数为zeroValue：计算的初始值
    //       用于在分区内进行计算时，当作初始值使用。
    // 第二个参数列表中传递参数为
    //       seqOp ：分区内的计算规则,相同key的value的计算
    //       combOp ：分区间的计算规则,相同key的value的计算
    val redult: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    println(redult.collect().mkString(","))

  }

  @Test
  def testfoldByKey(): Unit = {
    val rdd = sc.makeRDD(
      List(
        ("a", 1), ("a",2), ("c",3),
        ("b", 4), ("c",5), ("c",6)
      )
      ,2
    )

    // 如果分区内计算规则和分区间的计算规则相同都是求和，那么可以计算wordcount
    //        val result: RDD[(String, Int)] = rdd.aggregateByKey(0)(
    //            (x, y) => x + y,
    //            (x, y) => x + y
    //        )
    // val result= rdd.aggregateByKey(0)(_ + _, _ + _)
    // println(result.collect().mkString(","))

    // 如果分区内计算规则和分区间计算规则相同，那么可以将aggregateByKey简化为
    // 另外一个方法foldByKey
    val result = rdd.foldByKey(0)(_ + _)
    println(result.collect().mkString(","))

    // scala
    // List().reduce(_+_)
    // LIst().fold(0)(_+_)
    // spark
    // rdd.reduceByKey(_+_)
    // rdd.foldByKey(0)(_+_)
  }


  /**
   *  val result5: RDD[(Int, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
   *  val result4: RDD[(Int, Int)] = rdd.foldByKey(0)(_ + _)
   *  //等价于
   *  val result3: RDD[(Int, Int)] = rdd.reduceByKey(_ + _)
   */

  // TODO combineByKey
  @Test
  def testcombineByKey(): Unit = {

    // TODO 每个key的平均值 : 相同key的总和 / 相同key的数量

    // 0 =>【("a", 88), ("b", 95), ("a", 91)】
    // 1 =>【("b", 93), ("a", 95), ("b", 98)】
    val rdd = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2
    )

    //rdd.reduceByKey(_+_) // 88，91=179
    //rdd.aggregateByKey(0)(_+_, _+_)
    // 88 => (88,1) + 91 => (179,2) + 95 => (274, 3)
    // 计算时需要将value的格式发生改变，只需要第一个v发生改变结构即可
    // 如果计算时发现相同的key的value不符合计算规则的格式的话，那么选择combineByKey

    // TODO combineByKey方法可以传递3个参数
    //  第一个参数表示的就是将计算的第一个值转换结构
    //  第二个参数表示分区内的计算规则
    //  第三个参数表示分区间的计算规则

    /**
     *     val result1: RDD[(Int, Int)] = rdd.aggregateByKey(10)(_ + _, _ + _)
     *     //等价于
     *     val result2: RDD[(Int, Int)]  rdd.combineByKey(v => v + 10, (zero: Int, v) => zero + v, (zero1: Int, zero2: Int) => zero1 + zero2)
     */

    val result: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    result.map{
      case (key,(total,cnt)) =>{
        (key,total/cnt)
      }
    }.collect().foreach(println)

  }

  implicit  var i :Int =10
  // implicit  var j :Int =10
  //冥界(隐式)召唤

  @Test
  def testImplicitly() : Unit ={

    //使用冥界召唤，从当前方法的作用域中，获取指定类型的隐式变量
    val i1: Int = implicitly[Int]

    println(i1)

  }


  /**
   *       所有的Join都有shuffle
   *       Join:
   *       leftJoin
   *       rightjoin
   *       full join
   *
   *      join方法可以将两个rdd中相同的key的value连接在一起
   *      join方法性能不太高，能不用尽量不要用。
   */
  @Test
  def testjoin(): Unit = {

    val rdd1 = sc.makeRDD(
      List(
        ("a",1), ("b", 2),("c",3),("c",4)
      )
    )
    val rdd2 = sc.makeRDD(
      List(
        ("a",4), ("b", 5), ("b", 6)
      )
    )

    // rdd1.join(rdd2,1).saveAsTextFile("output")
    //rdd1.leftOuterJoin(rdd2,1).saveAsTextFile("output")
    //rdd1.rightOuterJoin(rdd2,1).saveAsTextFile("output")
    //rdd1.fullOuterJoin(rdd2,1).saveAsTextFile("output")

    // todo cogroup
    // cogroup:对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
    val result1: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    result1.collect().foreach(println)

  }



     //  todo  pipe ：  允许通过shell脚本处理RDD分区中的数据
    //  每个分区都会调用一次脚本，之后可以在脚本中使用echo将处理后的数据输出！
  /**
   * [root@linux1 data]# vim pipe.sh
   * #!/bin/sh
   * echo "Start"
   * while read LINE; do
   *    echo ">>>"${LINE}
   * done
   *
   * [root@linux1 data]# chmod 777 pipe.sh
   *
   *
   * scala> val rdd = sc.makeRDD(List("hi","Hello","how","are","you"), 1)
   *
   * scala> rdd.pipe("/opt/module/spark/pipe.sh").collect()
   * res18: Array[String] = Array(Start, >>>hi, >>>Hello, >>>how, >>>are, >>>you)
   */
  @Test
  def testPipe() : Unit ={

    val rdd= sc.makeRDD(List(11, 2, 33, 4), 2)

    println(rdd.pipe("/opt/module/spark/pipe.sh").collect().mkString(","))

  }



  @After
  def stop(): Unit={
    sc.stop ()
  }

}

  case class Person(name:String,age:Int)

  case class Person1(name:String,age:Int) extends  Ordered[Person1] {
    // 先按照名称降序比，再按照年龄升序比较
    override def compare(that: Person1): Int = {

      var result: Int = -this.name.compareTo(that.name)

      if (result == 0){
        result= this.age.compareTo(that.age)
      }
      result
    }
  }

  // TODO 自定义分区器
  // 1. 和Partitioner类发生关联，继承Partitioner
  // 2. 重写方法
  class MyPartitioner(num: Int) extends Partitioner {
    // 获取分区的数量
    override def numPartitions: Int = {
      num
    }

    // 根据数据的key来决定数据在哪个分区中进行处理
    // 方法的返回值来表示分区编号（索引）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case _ => 1
      }
    }

  }

class  MyPartitioner2(num:Int) extends  Partitioner {

  override def numPartitions: Int = num

  // 分区   按照年龄进行分区
  override def getPartition(key: Any): Int = {

    if (!key.isInstanceOf[Person]){
      0
    }else{
      val p: Person = key.asInstanceOf[Person]
      p.age % numPartitions
    }

  }
}




























package com.atguigu.SparkCore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

/**
 * Created by VULCAN on 2020/9/5
 *
 * Kryo:  高效的序列化框架！替换java.io.serilizable
 *            Serializable 在JDK1.1出现，在设计时，没有考虑大数据网络传输的问题！
 *                    Serializable在保存对象的信息时，除了属性的值，之外还会额外保存其他信息（类的继承关系等）
 *
 *              在大数据领域，只关心数据的值，不关心数据的类型，继承关系等！
 *
 *              希望在序列化时，可以仅仅保存数据的值的信息！
 *
 *              Hadoop:   Writable 替代 java.io.serilizable
 *              Spark : 不支持Writable，使用 Kryo作为序列化框架！
 *                          使用Kryo，类必须实现java.io.serilizable
 *                          Kryo基于 java.io.serilizable工作！
 *
 *                          Kryo的效率是java.io.serilizable的10倍以上
 */

//class User(age:Int=10) extends  Serializable

//case class User(age:Int=10)

//体会kryo
case class User2(age:Int=10,name:String="fhaowiehfaoiwhfoiua;whfeiawofh oi;aweh foiawhfoikjauwhfoi;auwhfeoiu;awehfaoiu;wehfaoiewu;fh")


class User6(age:Int=10) {
  //过滤RDD中 10 以下的元素返回新的RDD
  def filterRdd(rdd: RDD[Int]): RDD[Int] ={
    //局部变量，声明在方法中
    val myage= age
    // 算子 age称为闭包变量 需要将User对象序列化，发送到Executor，才能使用User.age！
    rdd.filter(x=> {
      x> myage
    })
  }
}


//方法序列化
class User1(age:Int=10){

  // 方法 也是 类的一个成员，不能单独存在，依附于对象
  def fun1(x:Int) : Boolean={

    x>10
  }

  def filterRdd(rdd: RDD[Int]): RDD[Int] ={

    // 函数
    /* def fun1(x:Int) : Boolean={
       x>10
     }*/
    // 算子 fun1称为闭包变量 需要将User对象序列化，发送到Executor，才能使用User.fun1！
    // 就近原则
    rdd.filter(x=>x>10)
  }
}

class RDD_Seria3 {

  @Test
  def test5() : Unit ={

    // 658.0 B
    // Kryo : 302.0 B
    val list = List(User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2()
      , User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(),
      User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2(), User2())

    val rdd: RDD[User2] = sparkContext.makeRDD(list, 2)

    // 使用带shuffle的算子测试序列化
    val rdd1: RDD[(User2, Iterable[User2])] = rdd.groupBy(x => x)

    rdd1.collect()

    Thread.sleep(100000000)
  }

  /*
      方法的序列化
           解决： ①User1实现序列化
                  ②User1不想实现序列化，可以在要使用的方法中，将计算逻辑声明为函数！
                      函数在方法中声明，不是类的成员！

                      使用匿名函数也可以!
   */
  @Test
  def test4() : Unit ={
    val list = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sparkContext.makeRDD(list, 2)
    //Task not serializable
    val rdd1: RDD[Int] = new User1().filterRdd(rdd)
    rdd1.collect()

  }
  /*
      属性序列化
              属性必须依附于对象存在，对象是类的实例！ 类是对象的抽象！

              解决： ①User实现序列化
                    ②User不想实现序列化，可以使用方法中的局部变量接收要用的属性！
   */
  @Test
  def test3() : Unit ={
    val list = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sparkContext.makeRDD(list, 2)
    // Task not serializable
    val rdd1: RDD[Int] = new User6().filterRdd(rdd)
    rdd1.collect()

  }

  //体会kryo
  val sparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("My app")
    .registerKryoClasses(Array(classOf[User2])))

  @Before
  def start(){

    //删除output目录
    val fileSystem: FileSystem = FileSystem.get(new Configuration())

    val path = new Path("output")

    if (fileSystem.exists(path)){
      fileSystem.delete(path,true)
    }

  }

  @After
  def stop(){
    sparkContext.stop()
  }

  /*
        解决： ① extends  Serializable
              ②  使用case class(推荐)
   */
  @Test
  def test2() : Unit ={

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sparkContext.makeRDD(list, 2)

    val user = new User6()

    //闭包  要求闭包中使用的外部变量必须能够序列化，否则 报错Task not serializable
    rdd.foreach( x => {
      user
    })

  }

  @Test
  def test1() : Unit ={

    val list = List(1, 2, 3, 4)

    val rdd: RDD[Int] = sparkContext.makeRDD(list, 2)

    // Driver
    var sum:Int =0

    // 分布式运算   构成闭包后，检查sum是否支持序列化，如果支持序列化，
    // 此时创建sum的一个副本，将副本发送到Task执行的Executor，在executor上运算
    rdd.foreach( x => sum += x)

    // Driver端声明的sum，没有变化
    println(sum)

  }

}

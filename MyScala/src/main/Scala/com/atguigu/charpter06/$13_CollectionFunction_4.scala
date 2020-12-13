package com.atguigu.charpter06

/**
 * 高阶函数
 */
object $13_CollectionFunction_4 {
  def main(args: Array[String]): Unit = {
    //filter -过滤 -针对的是集合的每个元素 -保留的是函数返回值为true的数据 **************
    val list = List[Int](10, 20, 3, 5, 8, 10, 33)
    //println(list.filter(x => x % 2 == 0))
    println(list.filter(_ % 2 == 0))

    //foreach
    list.foreach(x=>println(x))
    list.foreach(println(_)) //x=>println(x+1)

    //方法就是函数
    //val func =
    //list.foreach(println)
    //groupBy -针对的是集合的每个元素 **************
    //场景：用于多对一场景
    val list2 = List[(String,Int,String)](
      ("zhangsan",20,"beijing"),
      ("lisi",20,"shenzhen"),
      ("wangwu",20,"shenzhen"),
      ("zhaoliu",20,"shenzhen"),
      ("tianqi",20,"beijing"),
      ("wangermazi",20,"shanghai")
    )

    val map:Map[String,List[(String,Int,String)]] = list2.groupBy(x=>x._3)
    println(map)

    //flatMap - 针对集合的每个元素 = map+flatten **************
    //场景：用于一对多场景
    val list13 = List[String]("hello word","hello python")
    //List[String](hello,word,hello,python)
    val list14 = list13.map(x=>x.split(" "))
    //List[List[String]](List[String](hello,word),List(hello,python))
    val list15 = list14.flatten
    println(list15)

    val list16 = list13.flatMap(x=>x.split(" "))
    println(list16)

    //flatteny 压平
    //场景：用于一对多的
    val list7 = List[List[Int]](
      List[Int](1,2,3,4),
      List[Int](5,6,7),
      List[Int](8,9,10)
    )

    val list8 = list7.flatten
    println(list8)

    val list9 = List[List[List[Int]]](
      List[List[Int]](List[Int](1,2),List[Int](3,4)),
      List[List[Int]](List[Int](5,6),List[Int](7,8)),
      List[List[Int]](List[Int](9,10),List[Int](11,12))
    )
    val list10 = list9.flatten.flatten
    println(list10)

    //sorted - 按照元素本身的大小进行排序，是升序排列
    val list17 = List[Int](1,5,1,10,3,20)
    val list18 = list17.sorted.reverse
    println(list18)

    //sortBy - 针对集合的每个元素，返回的是排序字段升序 **************
    val list19 = List[(String,Int)](("zhangsan",20),("lisi",10),("wangwu",15),("zhaoliu",30))
    val list20 = list19.sortBy(x=>x._2)
    println(list20)

    //sortWith - 集合的每两个元素进行比较
    //升序
    val list21 = list17.sortWith((x,y)=>x<y)
    println(list21)
    //降序
    val list22 = list17.sortWith((x,y)=>x>y)
    println(list22)

    //reduce **************
    //agg:  上一次的聚合结果
    //curr: 代表本次聚合的元素
    val list23 = list17.reduce((agg,curr)=>{
      println(s"agg=${agg} curr=${curr}")
      agg+curr
    })
    //val list17 = List[Int](2,5,1,10,3,20)
    //第一次执行的时候 agg=2  curr=5   agg+curr=7
    //第二次执行的时候 agg=7  curr=1   agg+curr=8
    //第三次执行的时候 agg=8  curr=10   agg+curr=18
    //第四次执行的时候 agg=18  curr=3   agg+curr=21
    //第五次执行的时候 agg=21  curr=20  agg+curr=41
    println(list23)

    //reduceRight
    val list24 = list17.reduceRight((curr,agg)=>{
      println(s"agg=${agg} curr=${curr}")
      agg+curr
    })
    println(list24)
    println("="*40)

    //fold agg的初始值为fold第一个参数列表的值
    val list25 = list17.fold(10)((agg,curr)=>{
      println(s"agg=${agg} curr=${curr}")
      agg+curr
    })

    //foldRight
    val list26 = list17.foldRight(100)((curr,agg)=>{
      println(s"agg=${agg} curr=${curr}")
      agg+curr
    })
  }
}









































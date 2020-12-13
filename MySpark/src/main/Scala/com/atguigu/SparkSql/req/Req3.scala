package com.atguigu.SparkSql.req

import java.text.DecimalFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}


object Req3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // todo 创建环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // todo 访问外置的Hive
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9820/user/hive/warehouse")
      .config(sparkConf).getOrCreate()

    spark.sql("use xianmu1")

    // todo 从Hive表中获取满足条件的数据
    spark.sql(
      """
        |select
        |   a.*,
        |   c.area,
        |   p.product_name,
        |   c.city_name
        |from user_visit_action a
        |join city_info c on c.city_id = a.city_id
        |join product_info p on p.product_id = a.click_product_id
        |where a.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")

    // todo 将数据根据区域和商品进行分组，统计商品点击的数量
    // 北京，上海，北京，深圳
    //******************************************
    // in : cityname ; String
    // buffer : 2结构，（total, map）
    // out : remark : String
    // （ 商品点击总和， 每个城市点击总和 ）
    // （ 商品点击总和， Map（城市，点击Sum） ）
    // 城市点击sum / 商品点击总和 %
    //输出： 北京21.2%，天津13.2%，其他65.6%
    // todo 创建自定义聚合函数
    val udaf = new CityRemarkUDAF
    // todo 注册聚合函数
    spark.udf.register("cityRemadk",udaf)

    spark.sql(
      """
        |select
        |    area,
        |    product_name,
        |    count(*) as clickCount,
        |    cityRemark(city_name)
        |from t1 group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")

    // TODO 将统计结果根据数量进行排序（降序）
    spark.sql(
      """
        |select
        |    *,
        |    rank() over( partition by area order by clickCount desc ) as rank
        |from t2
            """.stripMargin).createOrReplaceTempView("t3")

    // TODO 将组内排序后的结果取前3名
    spark.sql(
      """
        |select
        |   *
        |from t3
        |where rank <= 3
            """.stripMargin).show

    spark.stop
  }

  // 自定义城市备注聚合函数
  class CityRemarkUDAF extends UserDefinedAggregateFunction{
    // todo 输入的数据其实就是城市名称
    override def inputSchema: StructType = {
      StructType(Array(StructField("cityName",StringType)))
    }

    // todo 缓冲区中的数据应该为：totalcnt,Map[cityname,cnt]
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("totalcnt",LongType),
        StructField("citymap",MapType(StringType,LongType))
      ))
    }

    // todo 返回城市备注的字符串
    override def dataType: DataType = {
      StringType
    }

    override def deterministic: Boolean = true

    // todo 缓冲区的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      //buffer.update(0,0L)
      buffer(1) = Map[String,Long]()
    }

    // todo 更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val cityName: String = input.getString(0)
      // 点击总和需要增加
      buffer(0) = buffer.getLong(0)+1
      //城市点击增加
      val citymap: Map[String, Long] = buffer.getAs[Map[String, Long]](1)

      val newClickCount: Long = citymap.getOrElse(cityName, 0L) + 1
       buffer(1) = citymap.updated(cityName, newClickCount)

    }

    // todo 合并缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //合并点击数量总和
      buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
      //buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))

      // Scala语法
      //            val array = Array(1,2,3,4)
      //            array(0) = 1
      //            array.update(0,1)

      //合并城市点击map
      val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](1)
      //等价于val map1: collection.Map[String, Int] = buffer1.getMap[String, Int](1)
      //等价于
      val map2: Map[String, Long] = buffer2(1).asInstanceOf[Map[String, Long]]


      buffer1(1) = map1.foldLeft(map2){
        case (map,(k,v)) => {
          map.updated(k,map.getOrElse(k,0L)+v)
        }
      }
    }

    // todo 对缓冲区进行计算并返回备注信息
    override def evaluate(buffer: Row): Any = {
      val totalcnt: Long = buffer.getLong(0)
      val citymap: collection.Map[String, Long] = buffer.getMap[String, Long](1)

/*      if(citymap.size>2){

      }else{

      }*/

      val cityToCountList: List[(String, Long)] = citymap.toList.sortWith(
        (left, right) => left._2 > right._2
      ).take(2)

      val hasRest: Boolean = citymap.size>2

      var rest = 0L
      val s: StringBuilder = new StringBuilder

      cityToCountList.foreach{
        case(city,cnt) =>{
          var r =(cnt*100/totalcnt)
          s.append(city+""+r+"%,")
          rest = rest + r
        }
      }

      s.toString()+"其他："+(100-rest)+"%"

      if(hasRest){

      }else{
        s.toString()
      }

    }
  }
}

case class MyBuf(var sum: Int,var cityInfo:Map[String,Int])

class CityRemarkUDAF2 extends Aggregator[String,MyBuf,String]{

  override def zero: MyBuf = MyBuf(0,Map[String,Int]())

  override def reduce(b: MyBuf, a: String): MyBuf = {

    val cityName: String = a
    //让地区的广告点击总数+1
    b.sum=b.sum+1
    //更新城市的广告点击次数
    b.cityInfo.updated(cityName,b.cityInfo.getOrElse(cityName,0)+1)

    b
  }

  override def merge(b1: MyBuf, b2: MyBuf): MyBuf = {
    //将所有城市的点击次数合并完毕
    val map1: Map[String, Int] = b1.cityInfo
    val map2: Map[String, Int] = b2.cityInfo

    //将其中的一个map的所有的k—v对，合并到另一个Map中
    b1.cityInfo = map2.foldLeft(map1){
      case (resultMap,(cityName,clickCount)) =>{
        val totalClickCount: Int = resultMap.getOrElse(cityName,0)+clickCount
        resultMap.updated(cityName,totalClickCount)
      }
    }

    //合并地区点击总数
    b1.sum=b1.sum+b2.sum
    b1
  }

  override def finish(reduction: MyBuf): String = {
    //获取地区点击的总次数
    val totalClickCount: Int = reduction.sum
    //获取每个城市点击的次数，根据次数取前二点击的城市
    val top2City: List[(String, Int)] = reduction.cityInfo.toList.sortBy(-_._2).take(2)

    //计算其他城市的信息  todo 不严谨
    val otherClickCount: Int = totalClickCount - top2City(0)._2 - top2City(1)._2

    // 将这个结果，转为字符串
    val resultToWrite: List[(String, Int)] = top2City :+ ("其他", otherClickCount)

    val format = new DecimalFormat("0.00%")

    var str =""

    //北京21.2%，天津13.2%，其他65.6%
    for ((city,clickCount) <- resultToWrite) {
      str +=  city+format.format(clickCount.toDouble / totalClickCount ) +","
    }

    //返回最终结果
    str.substring(0,str.length - 1)

  }

  override def bufferEncoder: Encoder[MyBuf] = Encoders.product[MyBuf]

  override def outputEncoder: Encoder[String] = Encoders.STRING
}


/*
    Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
          配置文件的参数没有生效，需要讲hive-site.xml收到拷贝到target/classes/目录下
 */






































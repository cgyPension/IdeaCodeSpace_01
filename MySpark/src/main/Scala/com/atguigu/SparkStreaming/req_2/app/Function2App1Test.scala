package com.atguigu.SparkStreaming.req_2.app

import java.sql.{Connection, PreparedStatement}

import com.atguigu.SparkStreaming.req_2.beans.AdsInfo
import com.atguigu.SparkStreaming.req_2.utils.JDBCUtil

import org.apache.spark.streaming.dstream.DStream

/**
 *
 * 实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL
 *
 *    在Mysql中建表存储计算的结果： 日期，地区，城市，广告，点击量
 *                                日期，地区，城市，广告 作为联合主键
 *
 *     思路：   总流量。 有状态的计算！ 使用updateStageByKey算子！
 *              统计的数据的时间范围：   0:00 ----> 当前
 *                          统计时间：  15：00
 *                                      在15：00统计时，将今天 各地区各城市各广告的点击总流量都进行统计
 *
 *
 *              如何将统计的结果存入Mysql?
 *                    insert: 主键冲突?
 *                        sqoop:   updateMode=  默认updateOnly(翻译为 update )
 *                                      |  allowInsert（翻译为 insert into xxx ON DUPLICATE KEY）
 *
                         * INSERT INTO  `area_city_ad_count` VALUES('2020-09-14','华北','北京','101',3)
                         * ON DUPLICATE KEY UPDATE COUNT=VALUES(COUNT)
 *                    update?
 *
 *
 * 问题： 使用ck目录保存每个采集周期计算的结果的状态！会造成小文件！
 *            解决：  ① 在集群中部署，ck目录在hdfs。 可以先一个程序，程序监控ck目录，将已经过期的ck目录删除！
 *                          程序-----> 每间隔5分钟提交一个Job
 *                          监控程序----->删除当前时间前15分钟之前所有生成的ck目录
 *                   ② 从根本上解决，不使用ck目录记录state，自然不能使用updateStageByKey算子
 *                        怎么完成有状态的计算？  自己维护state(mysql,redis)!
 *                              a) 程序在计算当前 这批数据时，先从数据库中，查询state
 *                                    state + 当前计算的结果  合并 =  新的state
 *                              b) 将state写入数据库
 *
 *
 *
 *
 */
object Function2App1Test extends  BaseApp {

  def main(args: Array[String]): Unit = {

    runApp{

      //设置ck目录
      streamingContext.checkpoint("Function2App1Test")

      // 从kafka中读取实时数据
      val datas: DStream[AdsInfo] = getAllBeans(getDStreamFromKafka())

      // 封装需要的数据    ((日期，地区，城市，广告)，点击量)
      val ds1: DStream[((String, String, String, String), Int)] = datas.map(bean => ((bean.dayString, bean.area, bean.city, bean.adsId), 1))
        .updateStateByKey((values: Seq[Int], state: Option[Int]) => Some(values.sum + state.getOrElse(0)))

      //将统计的总流量进行写出
      ds1.foreachRDD(rdd =>{
        // 将一个分区作为一个整体，批处理
          rdd.foreachPartition(iter => {

            //获取数据库的连接
            val connection: Connection = JDBCUtil.getConnection()
            // 准备sql
            val sql=
              """
                |INSERT INTO  `area_city_ad_count` VALUES(?,?,?,?,?)
                |ON DUPLICATE KEY UPDATE COUNT=?
                |""".stripMargin

            //预编译sql，
            val ps: PreparedStatement = connection.prepareStatement(sql)

            // map是scala集合的map，不是算子
            //val ints: Iterator[Int] = iter.map {
           iter.foreach {
              case ((date, area, city, ads), count) => {
                //填充占位符
                ps.setString(1, date)
                ps.setString(2, area)
                ps.setString(3, city)
                ps.setString(4, ads)
                ps.setInt(5, count)
                ps.setInt(6, count)
                //执行插入
                ps.executeUpdate()
              }
            }

            // 使用迭代器，将迭代器迭代的每条记录放入一个List集合中
            //println(ints.toList)

            //分区中的每条数据都已经插入完毕，关闭相关的资源
            ps.close()
            connection.close()

          })
      })

    }


  }

}

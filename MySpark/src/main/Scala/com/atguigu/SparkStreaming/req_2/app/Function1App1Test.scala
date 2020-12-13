package com.atguigu.SparkStreaming.req_2.app



import java.sql.{Connection, PreparedStatement, ResultSet}

import com.atguigu.SparkStreaming.req_2.beans.AdsInfo
import com.atguigu.SparkStreaming.req_2.utils.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer



/**
 *
 * 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑。
 * 注：黑名单保存到MySQL中。
 *
 *    准备工作：  black_list (保存黑名单)
 *
 *    思路：   ①对每批数据，都进行计算，计算当前这批数据，每个用户对每个广告的点击次数
 *                  1600054291787,华北,北京,101,3 =>    ((day,用户id,广告id),1)  => ((day,用户id,广告id),N)
 *
 *             ②计算完每批数据后，需要自己维护状态
 *                  ((用户id,广告id),N) 写入到 mysql user_ad_count
 *                  注意：  将之前的值 和 当前的N 进行 累加，之后再插入到 user_ad_count
 *                INSERT INTO user_ad_count VALUES(?,?,?,?)
 *                ON DUPLICATE KEY UPDATE COUNT=COUNT+?
 *
 *            ③ 从  user_ad_count 查询最新的状态，将查询的结果进行判断
 *                  哪些用户当前最新的状态已经 超过 100
 *                  select userid from user_ad_count where count > 100
 *
 *
 *            ④ 将③查询的用户，插入black_list表中
 *                   INSERT INTO black_list VALUES(?)
 *                    ON DUPLICATE KEY UPDATE userid=?
 */
object Function1App1Test extends  BaseApp {

  def main(args: Array[String]): Unit = {

    runApp{

      val datas: DStream[AdsInfo] = getAllBeans(getDStreamFromKafka())

      //((day,用户id,广告id),N)
      val ds: DStream[((String, String, String), Int)] = datas.map(bean => ((bean.dayString, bean.userId, bean.adsId), 1))
        .reduceByKey(_ + _)

      ds.foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          //每个分区创建一个连接
          val connection: Connection = JDBCUtil.getConnection()

          val sql1 =
            """
              | INSERT INTO user_ad_count VALUES(?,?,?,?)
              |  ON DUPLICATE KEY UPDATE COUNT=COUNT+?
              |""".stripMargin

          val ps: PreparedStatement = connection.prepareStatement(sql1)

          //将当前的点击次数合并到最新的状态中
          iter.foreach{
            case ((day,userid,adsid),count) => {

              ps.setString(1,day)
              ps.setString(2,userid)
              ps.setString(3,adsid)
              ps.setInt(4,count)
              ps.setInt(5,count)

              ps.executeUpdate()

            }
          }

          ps.close()

          // 从  user_ad_count 查询最新的状态，将查询的结果进行判断
          val sql2=
            """
              | select userid from user_ad_count where count > 30
              |
              |""".stripMargin

          val ps2: PreparedStatement = connection.prepareStatement(sql2)

          val rs: ResultSet = ps2.executeQuery()

          //遍历结果，查询需要拉黑的人
          val needToAddToBL: ListBuffer[String] = ListBuffer[String]()

          while(rs.next()) {
            needToAddToBL.append( rs.getString("userid"))
          }

          ps2.close()

          // 将需要拉黑的人插入黑名单
          val sql3=
            """
              | INSERT INTO black_list VALUES(?)
              |   ON DUPLICATE KEY UPDATE userid=?
              |""".stripMargin

          val ps3: PreparedStatement = connection.prepareStatement(sql3)

          needToAddToBL.foreach(name => {
            ps3.setString(1,name)
            ps3.setString(2,name)
            ps3.executeUpdate()
          })

          ps3.close()
          connection.close()
        })
      })
    }
  }
}

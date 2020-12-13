package com.atguigu.SparkCore


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.{JdbcRDD, NewHadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

import scala.reflect.ClassTag

class RDD_ReadAndWrite {

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
    def testio() : Unit ={
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File - RDD")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("input/1.txt")
        rdd.saveAsTextFile("output")

        rdd.saveAsObjectFile("output1")
        val objectRDD: RDD[String] = sc.objectFile[String]("output1")

        rdd.map((_, 1)).saveAsSequenceFile("output2")
        val seqRDD: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output2")

    }

    /**
     * 读写JDBC 以Mysql为例：
     *          读：  从Mysql中读取数据，封装为RDD
     */
   /* class JdbcRDD[T:ClassTag](
                             sc:SparkContext,
                             getConnection:() => Connection,
                             sql:String,    //要查询的sql
                             lowerBound:Long,   //占位符的上下界，用来分区
                             upperBound:Long,
                             numPartitions:Int,     //分几个区
                             mapRow:(ResultSet) => T = JdbcRDD.resultSetToObjectArray //将ResultSet中的单行记录封装为一个T类型
                             )

    之前：
        while(rs.next){     // next不需要调，也不能手动调
            rs.getInt;
            rs.getString
        }*/

   @Test
    def testJDBCRead()={
        val rdd = new JdbcRDD[User](
            sc,
            () => DriverManager.getConnection("jdbc:mysql://localhost:3306/0508","root","123456"),
            "select*from t_job where id=? and id=?",
            3,
            9,
            3,
            (rs:ResultSet) => User(rs.getInt("id"),rs.getString("username"),rs.getString("password"))
        )
       rdd.saveAsTextFile("output")
   }

    @Test
    def testJDBCWrite() : Unit ={
        val list = List(User(0, "jack11", "123456"), User(1, "jack12", "123456"))
        val rdd: RDD[User] = sc.makeRDD(list, 2)

        rdd.foreachPartition(iter=>{
            //创建连接
            val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/0508", "root", "guodai")
            val sql:String = "insert into tbl_user(username,password) values (?,?)"

            val ps:PreparedStatement = connection.prepareStatement(sql)

            iter.foreach(user =>{
                ps.setString(1,user.username)
                ps.setString(2,user.password)

                ps.executeUpdate()
            })

            //关闭资源
            ps.close()
            connection.close()
        })
    }


    /**
    读写HBase：
                    ①Connection :  通过zk连接上HBase
                    ②从Connection中获取一个Table(代表一张表)
                    读：  Table.scan(Scan)
                          Table.get(Get)

                    写：   Table.put(Put)

                    过于原始，使用封装好的工具来读写！

                    MR中，MR提供了对HBase表读写的专门的输入格式和输出格式！

                    TableInputFormat： 用来读取HBase表中的内容
                                        K: ImmutableBytesWritable：  BytesWritable代表 Byte[]的Writeable类型
                                                  rowkey对应的 Byte[]
                                        V: Result  (一行记录，若干Cell的集合)

                                        RecordReader<ImmutableBytesWritable, Result>

                    TableOutputFormat： 用来向hbase写出数据
                                        K-V

                   读取HBase中的记录封装为RDD

                   如果使用mapreduce包下的API，需要创建NewHadoopRDD

      class NewHadoopRDD[K, V](
            sc : SparkContext,
            inputFormatClass: Class[_ <: InputFormat[K, V]],
            keyClass: Class[K],
            valueClass: Class[V],
            @transient private val _conf: Configuration)
      extends RDD
     */

    @Test
    def testHBaseRDD() : Unit ={

      //new Configuration 不行，不会读取HBase的配置文件

      // 读取了Hadoop默认的配置文件及自定义的配置文件，加上hbase默认的配置文件自定义的配置文件
      val conf: Configuration = HBaseConfiguration.create()

      //在配置中，告诉spark，读取的是哪个表
      conf.set(TableInputFormat.INPUT_TABLE,"t1")

      val rdd = new NewHadoopRDD[ImmutableBytesWritable, Result](
        sc,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result],
        conf
      )

      //遍历
      rdd.foreach{
        case (rowkey,result) =>{
          //取出所有的cell
          val cells: Array[Cell] = result.rawCells()
          //  CellUtil： 从cell中读取指定的属性(rowkey，列族名，列名，值)   Bytes: 将常见的Java类型和Byte[]进行转换
          for (cell <- cells) {

            println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)) + "    "+
              "cn:" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "    "+
              "value:" + Bytes.toString(CellUtil.cloneValue(cell)))
          }
        }
      }

    }

  /*
     hbase 写
         TableOutputFormat： 用来向hbase写出数据
                                     K:KEY 任意  采取Rowkey作为Key  ImmutableBytesWritable
                                     V：Mutation
                                           将写出的数据封装为一个Put对象

                                     RecordWriter<KEY, Mutation>

          saveAsNewAPIHadoopDataset(Configuration): RDD[(K-V)]
*/
  @Test
  def testHBaseWrite() : Unit ={

    val conf: Configuration = HBaseConfiguration.create()

    //设置要写出的表
    conf.set(TableOutputFormat.OUTPUT_TABLE,"t1")

    val job: Job = Job.getInstance(conf)
    // 设置使用的输出格式
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    // 设置Job最终写出的key-value类型
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val list = List(("r3", "cf1", "age", "20"), ("r4", "cf1", "age", "30"), ("r3", "cf1", "name", "marry1"), ("r4", "cf1", "name", "marry2"))

    val rdd: RDD[(String, String, String, String)] = sc.makeRDD(list, 2)

    val result: RDD[(ImmutableBytesWritable, Put)] = rdd.map {
      case (rk, cf, cq, v) => {

        val key = new ImmutableBytesWritable
        key.set(Bytes.toBytes(rk))

        val value = new Put(Bytes.toBytes(rk))
        value.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes(v))

        (key, value)
      }

    }

    result.saveAsNewAPIHadoopDataset(job.getConfiguration)

  }







    @After
    def stop(){
        sc.stop()
    }

}

case class User(id:Int,username:String,password:String)






































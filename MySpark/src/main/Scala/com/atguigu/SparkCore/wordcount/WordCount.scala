package com.atguigu.SparkCore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *      相对路径： main下的代码的相对路径相对于project
 *
 *      MR:       ①Job
 *                  Job.getInstance(Configuration)
 *                      Job.setMapper()...    设置Job，为Job添加配置信息
 *                      Job.watiForCompletion,基于配置，创建 应用上下文  JobContextImpl
 *
 *                      Context(上下文)：
 *                          上文： 记录Job的来龙，如何配置
 *                          下文： 记录Job的去脉，当前程序的运行逻辑
 *
 *                          Mapper.map(,MapContext)
 *                          Reducer.reduce(,ReduceContext)
 *
 *                          基于JobContextImpl，创建MapContext，ReduceContext
 *
 *                ②Mapper,Reducer,Combiner   编程模型
 *
 *      Spark:    ①Job
 *                    创建Job的上下文
 *                ②RDD，广播变量，累加器     编程模型
 *
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    // 创建Spark运行配置对象
    val conf = new SparkConf().setMaster("local").setAppName("My app")

    // 创建Spark上下文环境对象（连接对象）
    val sparkContext = new SparkContext(conf)

    // 实现业务操作，读取数据
    // RDD : 更适合并行计算的数据模型。  RDD[String]: RDD[一行内容]
    //  参数path可以指向单一的文件也可以指向文件目录
    val rdd: RDD[String] = sparkContext.textFile("MySpark/input")

    //将读取的内容进行扁平化操作,切分单词
    val rdd1: RDD[String] = rdd.flatMap(line => line.split(" "))

    //  将分词后的数据进行分组（单词）RDD[(String, Int)]: RDD[(单词，1)]
    val rdd2: RDD[(String, Int)] = rdd1.map(word => (word, 1))

    // reduceByKey:  将相同key对应的values进行reduce运算,聚合！
    val result: RDD[(String, Int)] = rdd2.reduceByKey((value1, value2) => value1 + value2)

    // 将数据聚合结果采集到内存中进行输出,result.collect()从RDD模型变为Array数组
    println(result.collect().mkString(","))

    // 运算完成后，选哟关闭sparkContext
    sparkContext.stop()

  }
}

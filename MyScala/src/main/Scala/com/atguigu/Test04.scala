package com.atguigu

import com.alibaba.fastjson.JSON
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

import scala.io.Source

object Test04 {

  def get(url: String) = {
    //1、创建Http client
    val client = new HttpClient()
    //2、创建method
    val method = new GetMethod(url)
    //3、发起请求
    val code = client.executeMethod(method)
    //4、判断是否请求成功，返回对应的数据
    if (200 == code) {
      method.getResponseBodyAsString
    } else {
      null
    }
  }

  /**
   * 统计各省市的原始请求数
   */
  def main(args: Array[String]): Unit = {

    //1、读取数据
    val data = Source.fromFile("D:\\Program Files\\feiq\\Recv Files\\pmt.json", "utf-8").getLines().toList.take(100)

    //2、三部曲：过滤、去重、列裁剪

    //过滤数据[过滤ip为空的数据]
    data.filter(json => {
      val obj = JSON.parseObject(json)
      val ip = obj.getString("ip")
      ip != null && ip != ""
    })

      //列裁剪[ip、processnode、requestmode]
      .map(json => {
        val obj = JSON.parseObject(json)
        val ip = obj.getString("ip")
        val url = "https://restapi.amap.com/v3/ip?key=f75418e64363b8a96d3565108638c5f1&ip=%s".format(ip)
        //3、通过ip获取省份、城市
        val result = get(url)
        var province: String = null
        var city: String = null
        if (result != null) {
          val objResult = JSON.parseObject(result)
          province = objResult.getString("province")
          city = objResult.getString("city")
        }
        println(city, province)
        val processnode = obj.getInteger("processnode")
        val requestmode = obj.getInteger("requestmode")
        (province, city, processnode, requestmode)
      })
      .filter {
        case (province, city, processnode, requestmode) =>
          null != province && city != null && "" != province && city != "" && province!="[]"
      }

      //4、按照省份、城市进行分组
      .groupBy {
        case (province, city, processnode, requestmode) =>
          (province, city)
      }

      //5、统计原始请求数
      .map(x => {
        val (province, city) = x._1
        val num = x._2.filter {
          case (province, city, processnode, requestmode) =>
            processnode == 1 && requestmode >= 2
        }.size
        (province, city, num)
      })

      //6、结果展示
      .foreach(println(_))
  }
}

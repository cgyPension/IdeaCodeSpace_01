package com.atguigu.SparkStreaming.req_1.application
import com.atguigu.SparkStreaming.req_1.controller.BlackListController
import com.atguigu.summer.framework.core._
object BlackListApplication extends App with TApplication {

  start("sparkStreaming"){
    val controller: BlackListController = new BlackListController
    controller.execute()
  }


}

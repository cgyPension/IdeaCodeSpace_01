package com.atguigu.SparkStreaming.req_1.application

import com.atguigu.SparkStreaming.req_1.controller.MockDataController
import com.atguigu.summer.framework.core._
object MockDataApplication extends App with TApplication {

  start("sparkStreaming"){
    val controller: MockDataController = new MockDataController
    controller.execute()
  }
}

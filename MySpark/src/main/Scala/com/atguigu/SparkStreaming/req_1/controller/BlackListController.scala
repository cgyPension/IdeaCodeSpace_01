package com.atguigu.SparkStreaming.req_1.controller

import com.atguigu.SparkStreaming.req_1.service.BlackListService
import com.atguigu.summer.framework.core._
class BlackListController extends TController {

  val blackListService: BlackListService = new BlackListService

  override def execute(): Unit = {
    val result = blackListService.analysis()
  }


}

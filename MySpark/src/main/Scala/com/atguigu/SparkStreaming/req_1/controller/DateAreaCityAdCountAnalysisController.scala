package com.atguigu.SparkStreaming.req_1.controller

import com.atguigu.SparkStreaming.req_1.service.DateAreaCityAdCountAnalysisService
import com.atguigu.summer.framework.core._
class DateAreaCityAdCountAnalysisController extends TController {

   val service: DateAreaCityAdCountAnalysisService = new DateAreaCityAdCountAnalysisService
  override def execute(): Unit = {
    val result: Any = service.analysis()
  }
}

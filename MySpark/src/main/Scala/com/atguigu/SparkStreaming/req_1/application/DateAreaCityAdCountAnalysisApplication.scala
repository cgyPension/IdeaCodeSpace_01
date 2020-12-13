package com.atguigu.SparkStreaming.req_1.application

import com.atguigu.SparkStreaming.req_1.controller.DateAreaCityAdCountAnalysisController
import com.atguigu.summer.framework.core._
object DateAreaCityAdCountAnalysisApplication extends App with TApplication {


  start("sparkStreaming"){
     val dateAreaCityAdCountAnalysisController: DateAreaCityAdCountAnalysisController = new DateAreaCityAdCountAnalysisController
    dateAreaCityAdCountAnalysisController.execute()
  }


}

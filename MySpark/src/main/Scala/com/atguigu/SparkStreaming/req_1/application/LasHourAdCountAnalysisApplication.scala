package com.atguigu.SparkStreaming.req_1.application

import com.atguigu.SparkStreaming.req_1.controller.LasHourAdCountAnalysisController
import com.atguigu.summer.framework.core.TApplication

object LasHourAdCountAnalysisApplication extends App with TApplication{

    start( "sparkStreaming" ) {
        val controller = new LasHourAdCountAnalysisController
        controller.execute()
    }
}

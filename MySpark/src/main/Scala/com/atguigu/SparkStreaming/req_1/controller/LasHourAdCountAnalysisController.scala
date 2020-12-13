package com.atguigu.SparkStreaming.req_1.controller

import com.atguigu.SparkStreaming.req_1.service.LasHourAdCountAnalysisService
import com.atguigu.summer.framework.core.TController

class LasHourAdCountAnalysisController extends TController{

    private val lasHourAdCountAnalysisService = new LasHourAdCountAnalysisService

    override def execute(): Unit = {
        val result = lasHourAdCountAnalysisService.analysis()
    }
}

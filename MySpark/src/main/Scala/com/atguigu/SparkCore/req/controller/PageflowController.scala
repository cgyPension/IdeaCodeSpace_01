package com.atguigu.SparkCore.req.controller


import com.atguigu.SparkCore.req.service.PageflowService
import com.atguigu.summer.framework.core.TController

class PageflowController extends TController{

    private val pageflowService = new PageflowService

    override def execute(): Unit = {
        val result = pageflowService.analysis()
    }
}

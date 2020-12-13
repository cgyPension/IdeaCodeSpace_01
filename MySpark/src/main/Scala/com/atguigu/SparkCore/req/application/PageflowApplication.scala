package com.atguigu.SparkCore.req.application


import com.atguigu.SparkCore.req.controller.PageflowController
import com.atguigu.summer.framework.core.TApplication

object PageflowApplication extends App with TApplication{

    start( "spark" ) {
        val controller = new PageflowController
        controller.execute()
    }
}

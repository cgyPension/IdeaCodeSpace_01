package com.atguigu.SparkStreaming.req_1.controller

import com.atguigu.SparkStreaming.req_1.service.MockDataService
import com.atguigu.summer.framework.core.TController
class MockDataController extends TController{

  private val mockDataService: MockDataService = new MockDataService
  override def execute(): Unit = {
    val result = mockDataService.analysis()
  }
}

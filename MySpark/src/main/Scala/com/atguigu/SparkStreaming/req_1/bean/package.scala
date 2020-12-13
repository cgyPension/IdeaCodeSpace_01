package com.atguigu.SparkStreaming.req_1

package object bean {

  case class Ad_Click_Log(
                           ts:String,
                           area:String,
                           city:String,
                           userid:String,
                           adid:String )

}

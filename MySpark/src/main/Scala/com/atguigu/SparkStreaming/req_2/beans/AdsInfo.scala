package com.atguigu.SparkStreaming.req_2.beans

import java.text.SimpleDateFormat
import java.util.Date
;

// 1600054291787,华北,北京,101,3
case class AdsInfo(ts: Long,
        area: String,
        city: String,
        userId: String,
        adsId: String,
        var dayString: String = null, // yyyy-MM-dd
        var hmString: String = null) { // hh:mm

        val date = new Date(ts)
        dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
        hmString = new SimpleDateFormat("HH:mm").format(date)
}

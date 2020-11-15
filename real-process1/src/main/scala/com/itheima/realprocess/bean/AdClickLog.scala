package com.itheima.realprocess.bean

import com.alibaba.fastjson.JSON

/**
  * 将消息对象转为样例类
  */
case class AdClickLog(
                       var city: String,
                       var ad_campaigns: String,
                       var ad_media: String,
                       var ad_source: String,
                       var corpurin: String,
                       var device_type: String,
                       var host: String,
                       var t_id: String,
                       var user_id: String,
                       var click_user_id: String,
                       var timestamp: String
                     )


object AdClickLog {
  def apply(json: String): AdClickLog = {
    val obj = JSON.parseObject(json)
    new AdClickLog(
      obj.getString("city"),
      obj.getString("ad_campaign"),
      obj.getString("ad_media"),
      obj.getString("ad_source"),
      obj.getString("corpurin"),
      obj.getString("device_type"),
      obj.getString("host"),
      obj.getString("t_id"),
      obj.getString("user_id"),
      obj.getString("click_user_id"),
      obj.getString("timestamp")
    )
  }
}

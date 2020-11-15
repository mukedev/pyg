package com.itheima.realprocess.bean

/**
  * 将消息对象转为样例类
  */
case class AdClickLogWide(
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
                           var timestamp: String,
                           var is_new: Long,
                           var click_cnt: Long
                         )




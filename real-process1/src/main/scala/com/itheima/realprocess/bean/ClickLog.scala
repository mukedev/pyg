package com.itheima.realprocess.bean

/**
  * 将点击流日志转为样例类
  */

import com.alibaba.fastjson.JSON

// 频道ID(channelID)
// 产品类别ID(categoryID)
// 产品ID(produceID)
// 国家(country)
// 省份(province)
// 城市(city)
// 网络方式(network)
// 来源方式(source)
// 浏览器类型(browserType)
// 进入网站时间(entryTime)
// 离开网站时间(leaveTime)
// 用户的ID(userID)
case class ClickLog(
                     var channelID: String,
                     var categoryID: String,
                     var produceID: String,
                     var country: String,
                     var province: String,
                     var city: String,
                     var network: String,
                     var source: String,
                     var browserType: String,
                     var entryTime: String,
                     var leaveTime: String,
                     var userID: String
                   )

object ClickLog {
  def apply(json: String): ClickLog = {
    // 先把json 转为JSONObject
    val jsonObject = JSON.parseObject(json)

    // 提取jsonObject中的各个属性，赋值给样例类
    var channelID = jsonObject.getString("channelID")
    var categoryID = jsonObject.getString("categoryID")
    var produceID = jsonObject.getString("produceID")
    var country = jsonObject.getString("country")
    var province = jsonObject.getString("province")
    var city = jsonObject.getString("city")
    var network = jsonObject.getString("network")
    var source = jsonObject.getString("source")
    var browserType = jsonObject.getString("browserType")
    var entryTime = jsonObject.getString("entryTime")
    var leaveTime = jsonObject.getString("leaveTime")
    var userID = jsonObject.getString("userID")

    ClickLog(
      channelID,
      categoryID,
      produceID,
      country,
      province,
      city,
      network,
      source,
      browserType,
      entryTime,
      leaveTime,
      userID
    )
  }
}
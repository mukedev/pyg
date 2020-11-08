package com.itheima.syncdb.bean

import com.alibaba.fastjson.JSON

/**
  * 定义原始 Canal消息 样例类
  *
  * @param emptyCount
  * @param logFileName
  * @param dbName
  * @param logFileOffset
  * @param eventType
  * @param columnValueList
  * @param tableName
  * @param timestamp
  */
case class Canal(var emptyCount: Long,
                 var logFileName: String,
                 var dbName: String,
                 var logFileOffset: Long,
                 var eventType: String,
                 var columnValueList: String,
                 var tableName: String,
                 var timestamp: Long
                )

object Canal {

  def apply(json: String): Canal = {
    var canal:Canal=null
    try {
     canal = JSON.parseObject[Canal](json, classOf[Canal])
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    canal
  }


  def main(args: Array[String]): Unit = {
    val json = "{\"emptyCount\":2,\"logFileName\":\"mysql- bin.000002\",\"dbName\":\"pyg\",\"logFileOffset\":250,\"eventType\":\"INSERT\",\"columnValue List\":[{\"columnName\":\"commodityId\",\"columnValue\":\"1\",\"isValid\":\"true\"}, {\"columnName\":\"commodityName\",\"columnValue\":\"耐克\",\"isValid\":\"true\"}, {\"columnName\":\"commodityTypeId\",\"columnValue\":\"1\",\"isValid\":\"true\"}, {\"columnName\":\"originalPrice\",\"columnValue\":\"888.0\",\"isValid\":\"true\"}, {\"columnName\":\"activityPrice\",\"columnValue\":\"820.0\",\"isValid\":\"true\"}],\"tableNa me\":\"commodity\",\"timestamp\":1553741346000}"
    println(Canal(json).dbName)
  }
}
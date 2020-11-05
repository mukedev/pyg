package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide, Message}
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
  * 预处理任务
  */
object PreprocessTask {

  def process(watermarkDataStream: DataStream[Message]) = {

    watermarkDataStream.map {
      msg =>
        // 转换时间
        val yearMonth = FastDateFormat.getInstance("yyyyMM").format(msg.timestamp)
        val yearMonthDay = FastDateFormat.getInstance("yyyyMMdd").format(msg.timestamp)
        val yearMonthDayHour = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timestamp)

        // 转换地区
        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city

        val tuple = isNewProcess(msg)

        ClickLogWide(
          msg.clickLog.channelID,
          msg.clickLog.categoryID,
          msg.clickLog.produceID,
          msg.clickLog.country,
          msg.clickLog.province,
          msg.clickLog.city,
          msg.clickLog.network,
          msg.clickLog.source,
          msg.clickLog.browserType,
          msg.clickLog.entryTime,
          msg.clickLog.leaveTime,
          msg.clickLog.userID,
          msg.count,
          msg.timestamp,
          address,
          yearMonth,
          yearMonthDay,
          yearMonthDayHour,
          tuple._1,
          tuple._2,
          tuple._3,
          tuple._4
        )
    }
  }

  def isNewProcess(msg:Message) = {
    // 定义四个变量，初始化为0
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0

    // 从HBase中查询用户记录，如果有记录，再去判断其它时间，如果没有记录，则证明是新用户
    val tableName = "user_history"
    var colFamily = "info"
    var rowkey = msg.clickLog.userID + msg.clickLog.channelID
    var userIdColumn = "userid"

    var channelIdColumn = "channelid"

    var lastVisitedTimeColumn = "lastVisitedTime"

    val userId: String = HBaseUtil.getData(tableName, rowkey, colFamily,userIdColumn)
    val channelId: String = HBaseUtil.getData(tableName, rowkey, colFamily,channelIdColumn)
    val lastVisitedTime: String = HBaseUtil.getData(tableName, rowkey, colFamily,lastVisitedTimeColumn)


    // 如果userid为空，则该用户是新用户
    if (StringUtils.isBlank(userId)){
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1

      // 保存用户的访问记录到"user_history"
      HBaseUtil.putMapData(tableName, rowkey, colFamily,Map(
          userIdColumn -> msg.clickLog.userID,
          channelIdColumn -> msg.clickLog.channelID,
          lastVisitedTimeColumn -> msg.timestamp
      ))
    } else {
      isNew = 0
      //其它字段需要进行时间戳比对
      isHourNew = compareDate(msg.timestamp,lastVisitedTime.toLong,"yyyyMMddHH")
      isDayNew = compareDate(msg.timestamp,lastVisitedTime.toLong,"yyyyMMdd")
      isMonthNew = compareDate(msg.timestamp,lastVisitedTime.toLong,"yyyyMM")

      //更新"user_history"的用户时间戳
      HBaseUtil.putData(tableName,rowkey,colFamily,lastVisitedTimeColumn,msg.timestamp.toString)
    }

    (isNew,isHourNew,isDayNew,isMonthNew)
  }


  /**
    * 对比时间戳
    * @param currentTime
    * @param historyTime
    * @param format
    */
  def compareDate(currentTime:Long, historyTime:Long, format:String): Int ={
    val currentTimeStr = timestampToStr(currentTime, format)
    val historyTimeStr = timestampToStr(historyTime, format)

    // 比对字符串的大小，如果当前时间> 历史时间返回1
    var result: Int = currentTimeStr.compareTo(historyTimeStr)
    if (result > 0){
      result = 1
    } else {
      result = 0
    }
    result
  }

  /**
    * 转换日期
    * @param timestamp
    * @param format
    * @return
    */
  def timestampToStr(timestamp:Long, format:String): String ={
    FastDateFormat.getInstance(format).format(timestamp)
  }

}

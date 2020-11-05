package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time

case class ChannelBrowser(
                           var channelId: String,
                           var browser: String,
                           var date: String,
                           var pv: Long,
                           var uv: Long,
                           var newCount: Long,
                           var oldCount: Long
                         )

/**
  * 实时渠道浏览器分析业务开发
  */
object ChannelBrowserTask extends BaseTask[ChannelBrowser] {

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelBrowser] = {
    val mapDataStream: DataStream[ChannelBrowser] = clickLogWideDataStream.flatMap {
      clickLogWide =>
        val isOld = (isNew: Long, isDateNew: Long) => if (isNew == 0 && isDateNew == 1) 0 else 1
        List(
          ChannelBrowser(
            clickLogWide.channelID,
            clickLogWide.browserType,
            clickLogWide.yearMonthDayHour,
            clickLogWide.count,
            clickLogWide.isHourNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isHourNew)
          )
        )
    }
    mapDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelBrowser]): KeyedStream[ChannelBrowser, String] = {
    val keyedDataStream = mapDataStream.keyBy {
      browser => browser.channelId + browser.browser + browser.date
    }
    keyedDataStream
  }

  override def timeWindow(keyedDataStream: KeyedStream[ChannelBrowser, String]): WindowedStream[ChannelBrowser, String, TimeWindow] = {
    val windowDataStream: WindowedStream[ChannelBrowser, String, TimeWindow] = keyedDataStream.timeWindow(Time.seconds(3))
    windowDataStream
  }

  override def reduce(windowDataStream: WindowedStream[ChannelBrowser, String, TimeWindow]): DataStream[ChannelBrowser] = {
    val reducedDataStream = windowDataStream.reduce {
      (t1: ChannelBrowser, t2: ChannelBrowser) =>
        ChannelBrowser(t1.channelId, t1.browser, t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount
        )
    }
    reducedDataStream
  }

  override def sinkToHBase(reducedDataStream: DataStream[ChannelBrowser]): Unit = {
    reducedDataStream.addSink(new SinkFunction[ChannelBrowser] {
      override def invoke(value: ChannelBrowser, context: SinkFunction.Context[_]): Unit = {
        val tableName = "channel_browser"
        val cfName = "info"
        val rowkey = value.channelId + ":" + value.browser + ":" + value.date
        val colChannelId = "channelId"
        val colBrowser = "browser"
        val colDate = "date"
        val colPv = "pv"
        val colUv = "uv"
        val colNewCount = "newCount"
        val colOldCount = "oldCount"

        val mapResult: Map[String, Any] = HBaseUtil.getMapData(tableName, rowkey, cfName, List(
          colPv, colUv, colNewCount, colOldCount
        ))

        var totalPv = 0l
        var totalUv = 0l
        var totalNewCount = 0l
        var totalOldCount = 0l

        if (mapResult != null && mapResult.size > 0 && StringUtils.isNotBlank(mapResult(colPv).toString)) {
          totalPv = value.pv + (mapResult(colPv).toString.toLong)
        } else {
          totalPv = value.uv
        }
        if (mapResult != null && mapResult.size > 0 && StringUtils.isNotBlank(mapResult(colUv).toString)) {
          totalUv = value.uv + (mapResult(colUv).toString.toLong)
        } else {
          totalUv = value.uv
        }
        if (mapResult != null && mapResult.size > 0 && StringUtils.isNotBlank(mapResult(colNewCount).toString)) {
          totalNewCount = value.newCount + mapResult(colNewCount).toString.toLong
        } else {
          totalNewCount = value.newCount
        }
        if (mapResult != null && mapResult.size > 0 && StringUtils.isNotBlank(mapResult(colOldCount).toString)) {
          totalOldCount = value.oldCount + mapResult(colOldCount).toString.toLong
        } else {
          totalOldCount = value.oldCount
        }

        HBaseUtil.putMapData(tableName, rowkey, cfName, Map(
          colChannelId -> value.channelId,
          colBrowser -> value.browser,
          colDate -> value.date,
          colPv -> value.pv,
          colUv -> value.uv,
          colNewCount -> value.newCount,
          colOldCount -> value.oldCount
        ))
      }
    })
  }
}

package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 渠道用户新鲜度
  *
  * @param channelId
  * @param date
  * @param newCount
  * @param oldCount
  */

case class ChannelFreshness(
                             var channelId: String,
                             var date: String,
                             var newCount: Long,
                             var oldCount: Long
                           )

/**
  * 1.转换
  * 2.分组
  * 3.时间窗口
  * 4.聚合
  * 5.落地HBase
  */
object ChannelFreshnessTask extends BaseTask [ChannelFreshness]{

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelFreshness] = {
    val channelFreshnessDataStream: DataStream[ChannelFreshness] = clickLogWideDataStream.flatMap {
      clickLogWide =>
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

        List(
          ChannelFreshness(clickLogWide.channelID, clickLogWide.yearMonthDayHour, clickLogWide.isNew, isOld(clickLogWide.isNew, clickLogWide.isHourNew)),
          ChannelFreshness(clickLogWide.channelID, clickLogWide.yearMonthDay, clickLogWide.isNew, isOld(clickLogWide.isNew, clickLogWide.isDayNew)),
          ChannelFreshness(clickLogWide.channelID, clickLogWide.yearMonth, clickLogWide.isNew, isOld(clickLogWide.isNew, clickLogWide.isMonthNew))
        )
    }
    channelFreshnessDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelFreshness]): KeyedStream[ChannelFreshness, String] = {
    val keyedDataStream: KeyedStream[ChannelFreshness, String] = mapDataStream.keyBy {
      channelFreshness => channelFreshness.channelId + channelFreshness.date
    }
    keyedDataStream
  }

  override def timeWindow(keyedDataStream: KeyedStream[ChannelFreshness, String]): WindowedStream[ChannelFreshness, String, TimeWindow] = {
    val windowDataStream: WindowedStream[ChannelFreshness, String, TimeWindow] = keyedDataStream.timeWindow(Time.seconds(3))
    windowDataStream
  }

  override def reduce(windowDataStream: WindowedStream[ChannelFreshness, String, TimeWindow]): DataStream[ChannelFreshness] = {
    val reduceDataStream: DataStream[ChannelFreshness] = windowDataStream.reduce {
      (t1, t2) =>
        ChannelFreshness(t1.channelId, t1.date, t1.newCount + t2.newCount, t1.oldCount + t2.oldCount)
    }
    reduceDataStream
  }

  override def sinkToHBase(reducedDataStream: DataStream[ChannelFreshness]): Unit = {
    reducedDataStream.addSink(new SinkFunction[ChannelFreshness] {
      override def invoke(value: ChannelFreshness, context: SinkFunction.Context[_]): Unit = {
        val tableName = "channel_freshness"
        val cfName = "info"
        val colChannelId = "channelId"
        val colDate = "date"
        val newCount = "newCount"
        val oldCount = "oldCount"
        val rowkey = value.channelId + value.date

        val newCountValue = HBaseUtil.getData(tableName, rowkey, cfName, newCount)
        val oldCountValue = HBaseUtil.getData(tableName, rowkey, cfName, oldCount)

        var totalNew = 0l
        var totalOld = 0l
        if (StringUtils.isNotBlank(newCountValue)){
          totalNew = value.newCount + newCountValue.toLong
        } else {
          totalNew = value.newCount
        }

        if (StringUtils.isNotBlank(oldCountValue)){
          totalOld = value.oldCount + oldCountValue.toLong
        } else {
          totalOld = value.oldCount
        }

        HBaseUtil.putMapData(tableName, rowkey, cfName, Map(
          colChannelId -> value.channelId,
          colDate -> value.date,
          newCount -> totalNew,
          oldCount -> totalOld
        ))
      }
    })
  }
}

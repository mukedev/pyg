package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class ChannelPvUv(
                        val channelId: String,
                        val yearDayMonthHour: String,
                        val pv: Long,
                        val uv: Long
                      )

/**
  * 渠道pv/uv
  *
  * 1.转换
  * 2.分组
  * 3.时间窗口
  * 4.聚合
  * 5.落地
  *
  */
object ChannelPvUvTask extends BaseTask[ChannelPvUv]{

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelPvUv] = {
    val channelPvUvDataStream = clickLogWideDataStream.flatMap {
      clickLogWide =>
        List(
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDayHour, clickLogWide.count, clickLogWide.isHourNew),
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDay, clickLogWide.count, clickLogWide.isDayNew),
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonth, clickLogWide.count, clickLogWide.isMonthNew)
        )
    }
    channelPvUvDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelPvUv]): KeyedStream[ChannelPvUv, String] = {
    val keyedDataStream: KeyedStream[ChannelPvUv, String] = mapDataStream.keyBy {
      channlPvUv => channlPvUv.channelId + channlPvUv.yearDayMonthHour
    }
    keyedDataStream
  }

  override def timeWindow(keyedDataStream: KeyedStream[ChannelPvUv, String]): WindowedStream[ChannelPvUv, String, TimeWindow] = {
    val windowDataStream: WindowedStream[ChannelPvUv, String, TimeWindow] = keyedDataStream.timeWindow(Time.seconds(3))
    windowDataStream
  }

  override def reduce(windowDataStream: WindowedStream[ChannelPvUv, String, TimeWindow]): DataStream[ChannelPvUv] = {
    val reduceDataStream: DataStream[ChannelPvUv] = windowDataStream.reduce {
      (t1: ChannelPvUv, t2: ChannelPvUv) =>
        ChannelPvUv(t1.channelId, t2.yearDayMonthHour, t1.pv + t2.pv, t1.uv + t2.uv)
    }
    reduceDataStream
  }

  override def sinkToHBase(reducedDataStream: DataStream[ChannelPvUv]): Unit = {
    reducedDataStream.addSink(new SinkFunction[ChannelPvUv] {
      override def invoke(value: ChannelPvUv, context: SinkFunction.Context[_]): Unit = {
        val tableName = "channel_pvuv"
        val cfName = "info"
        val colChannelId = "channelId"
        val colYearMonthHour = "yearMonthDayHour"
        val colPv = "pv"
        val colUv = "uv"
        val rowkey = value.channelId +":"+ value.yearDayMonthHour

        val pvValue: String = HBaseUtil.getData(tableName,rowkey,cfName,colPv)
        val uvValue: String = HBaseUtil.getData(tableName,rowkey,cfName,colUv)

        var totalPv = 0l
        var totalUv = 0l

        if (StringUtils.isNotBlank(pvValue)){
          totalPv = value.pv + pvValue.toLong
        } else {
          totalPv = value.pv
        }

        if (StringUtils.isNotBlank(uvValue)){
          totalUv = value.uv + uvValue.toLong
        } else {
          totalUv = value.uv
        }

        // 保存/更新HBaseUtils
        HBaseUtil.putMapData(tableName, rowkey, cfName, Map(
          colChannelId -> value.channelId,
          colYearMonthHour -> value.yearDayMonthHour,
          pvValue -> totalPv,
          uvValue -> totalUv
        ))
      }
    })
  }
}

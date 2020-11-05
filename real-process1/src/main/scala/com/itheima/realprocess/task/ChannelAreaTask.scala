package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time

case class ChannelAreaTask(
                            var channelId: String,
                            var area: String,
                            var date: String,
                            var pv: Long,
                            var uv: Long,
                            var newCount: Long,
                            var oldCount: Long
                          )

object ChannelAreaTask extends BaseTask[ChannelAreaTask] {

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelAreaTask] = {
    val mapDataStream: DataStream[ChannelAreaTask] = clickLogWideDataStream.flatMap {
      clickLogWide =>
        var isOld = (isNew: Long, isDateNew: Long) => if (isNew == 0 && isDateNew == 1) 1 else 0

        List(ChannelAreaTask(
          clickLogWide.channelID,
          clickLogWide.address,
          clickLogWide.yearMonthDayHour,
          clickLogWide.count,
          clickLogWide.isHourNew,
          clickLogWide.isNew,
          isOld(clickLogWide.isNew, clickLogWide.isHourNew)
        ),
          ChannelAreaTask(
            clickLogWide.channelID,
            clickLogWide.address,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelAreaTask(
            clickLogWide.channelID,
            clickLogWide.address,
            clickLogWide.yearMonthDayHour,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          ))
    }
    mapDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelAreaTask]): KeyedStream[ChannelAreaTask, String] = {
    val keyedDataStream: KeyedStream[ChannelAreaTask, String] = mapDataStream.keyBy {
      area => area.channelId + ":" + area.area + ":" + area.date
    }
    keyedDataStream
  }

  override def timeWindow(keyedDataStream: KeyedStream[ChannelAreaTask, String]): WindowedStream[ChannelAreaTask, String, TimeWindow] = {
    val windowDataStream: WindowedStream[ChannelAreaTask, String, TimeWindow] = keyedDataStream.timeWindow(Time.seconds(3))
    windowDataStream
  }

  override def reduce(windowDataStream: WindowedStream[ChannelAreaTask, String, TimeWindow]): DataStream[ChannelAreaTask] = {
    val reducedDataStream: DataStream[ChannelAreaTask] = windowDataStream.reduce {
      (t1, t2) =>
        ChannelAreaTask(t1.channelId, t1.area, t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount)
    }
    reducedDataStream
  }

  override def sinkToHBase(reducedDataStream: DataStream[ChannelAreaTask]): Unit = {

    reducedDataStream.addSink(new SinkFunction[ChannelAreaTask] {
      override def invoke(value: ChannelAreaTask, context: SinkFunction.Context[_]): Unit =
      {
        val tableName = "channel_area"
        val cfName = "info"
        val colChannelId = "channelId"
        val colAddress = "area"
        val colDate = "date"
        val colPv = "pv"
        val colUv = "uv"
        val colNewCount = "newCount"
        val colOldCount = "oldCount"
        val rowkey = value.channelId + ":" + value.area + ":" + value.date

        val pvValue = HBaseUtil.getData(tableName,rowkey,cfName,colPv)
        val uvValue = HBaseUtil.getData(tableName,rowkey,cfName,colUv)
        val newValue = HBaseUtil.getData(tableName,rowkey,cfName,colNewCount)
        val oldValue = HBaseUtil.getData(tableName,rowkey,cfName,colOldCount)

        var totalPv = 0l
        var totalUv = 0l
        var totalNewCount = 0l
        var totalOldCount = 0l

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
        if (StringUtils.isNotBlank(newValue)){
          totalNewCount = value.newCount + newValue.toLong
        } else {
          totalNewCount = value.newCount
        }
        if (StringUtils.isNotBlank(oldValue)){
          totalOldCount = value.oldCount + oldValue.toLong
        } else {
          totalOldCount = value.oldCount
        }

        HBaseUtil.putMapData(tableName,rowkey,cfName,Map(
          colChannelId -> value.channelId,
          colAddress -> value.area,
          colDate -> value.date,
          colPv -> totalPv,
          colUv -> totalUv,
          colNewCount -> totalNewCount,
          colOldCount -> totalOldCount
        ))
      }
    })
  }
}

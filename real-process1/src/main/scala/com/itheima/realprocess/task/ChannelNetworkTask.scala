package com.itheima.realprocess.task
import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time

case class ChannelNetwork(
                         var channelId: String,
                         var network: String,
                         var date: String,
                         var pv: Long,
                         var uv: Long,
                         var newCount: Long,
                         var oldCount: Long
                         )

/**
  * 实时运营商分析业务开发
  */
object ChannelNetworkTask extends BaseTask[ChannelNetwork] {

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelNetwork] = {
    val mapDataStream: DataStream[ChannelNetwork] = clickLogWideDataStream.flatMap {
      clickLogWide =>
        val isOld = (isNew: Long, isDateNew: Long) => if (isNew == 0 && isDateNew == 1) 0 else 1
        List(
          ChannelNetwork(
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonthDayHour,
            clickLogWide.count,
            clickLogWide.isHourNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isHourNew)
          ),
          ChannelNetwork(
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelNetwork(
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonth,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          )
        )
    }
    mapDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelNetwork]): KeyedStream[ChannelNetwork, String] = {
    val keyedDataStream: KeyedStream[ChannelNetwork, String] = mapDataStream.keyBy {
      network => network.channelId + network.network + network.date
    }
    keyedDataStream
  }

  override def timeWindow(keyedDataStream: KeyedStream[ChannelNetwork, String]): WindowedStream[ChannelNetwork, String, TimeWindow] = {
    val windowDataStream: WindowedStream[ChannelNetwork, String, TimeWindow] = keyedDataStream.timeWindow(Time.seconds(3))
    windowDataStream
  }

  override def reduce(windowDataStream: WindowedStream[ChannelNetwork, String, TimeWindow]): DataStream[ChannelNetwork] = {
    val reducedDataStream: DataStream[ChannelNetwork] = windowDataStream.reduce {
      (t1: ChannelNetwork, t2: ChannelNetwork) =>
        ChannelNetwork(t1.channelId, t1.network, t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount
        )
    }
    reducedDataStream
  }

  override def sinkToHBase(reducedDataStream: DataStream[ChannelNetwork]): Unit = {

    reducedDataStream.addSink(new SinkFunction[ChannelNetwork] {
      override def invoke(value: ChannelNetwork, context: SinkFunction.Context[_]): Unit =
      {
        val tableName = "channel_network"
        val rowkey = value.channelId + ":" + value.network + ":" + value.date
        val cfName = "info"
        val colChannelId = "channelId"
        val colNetwork = "network"
        val colDate = "date"
        val colPv = "pv"
        val colUv = "uv"
        val colNewCount = "newCount"
        val colOldCount = "oldCount"

        val pvValue = HBaseUtil.getData(tableName, rowkey, cfName, colPv)
        val uvValue = HBaseUtil.getData(tableName, rowkey, cfName, colUv)
        val newCountValue = HBaseUtil.getData(tableName, rowkey, cfName, colNewCount)
        val oldCountValue = HBaseUtil.getData(tableName, rowkey, cfName, colOldCount)

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
        if (StringUtils.isNotBlank(newCountValue)){
          totalNewCount = value.newCount + newCountValue.toLong
        } else {
          totalNewCount = value.newCount
        }
        if (StringUtils.isNotBlank(oldCountValue)){
          totalOldCount = value.oldCount + oldCountValue.toLong
        } else {
          totalOldCount = value.oldCount
        }

        //保存到HBase
        HBaseUtil.putMapData(tableName, rowkey, cfName, Map(
          colChannelId -> value.channelId,
          colNetwork -> value.network,
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

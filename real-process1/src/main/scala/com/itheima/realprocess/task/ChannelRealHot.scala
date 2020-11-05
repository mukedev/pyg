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
  * 频道热点分析
  *
  * 1.字段转换
  * 2.分组
  * 3.时间窗口
  * 4.聚合
  * 5.落地HBase
  */
case class ChannelRealHot(var channelid: String, var visited: Long)

object ChannelRealHotTask extends BaseTask[ChannelRealHot] {

  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelRealHot] = {
    val realHotDataStream = clickLogWideDataStream.map {
      clickLogWide: ClickLogWide =>
        ChannelRealHot(clickLogWide.channelID, clickLogWide.count)
    }
    realHotDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelRealHot]): KeyedStream[ChannelRealHot, String] = {
    val keyedStream: KeyedStream[ChannelRealHot, String] = mapDataStream.keyBy(_.channelid)
    keyedStream
  }

  override def timeWindow(keyedDataStream: KeyedStream[ChannelRealHot, String]): WindowedStream[ChannelRealHot, String, TimeWindow] = {
    val windowDataStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyedDataStream.timeWindow(Time.seconds(3))
    windowDataStream
  }

  override def reduce(windowDataStream: WindowedStream[ChannelRealHot, String, TimeWindow]): DataStream[ChannelRealHot] = {
    val reduceDataStream: DataStream[ChannelRealHot] = windowDataStream.reduce {
      (t1: ChannelRealHot, t2: ChannelRealHot) =>
        ChannelRealHot(t1.channelid, t1.visited + t2.visited)
    }
    reduceDataStream
  }

  override def sinkToHBase(reducedDataStream: DataStream[ChannelRealHot]): Unit = {
    reducedDataStream.addSink(new SinkFunction[ChannelRealHot] {
      override def invoke(value: ChannelRealHot, context: SinkFunction.Context[_]): Unit = {
        // HBase的相关字段
        val tableName = "channel"
        val cfName = "info"
        val channelColumnl = "channelId"
        val visitedColumn = "visited"
        val rowkey = value.channelid

        // 查询是否在HBase中存在
        val visitedValue = HBaseUtil.getData(tableName, rowkey, cfName, visitedColumn)

        // 创建总数的临时变量
        var totalCount = 0l

        if (StringUtils.isNotBlank(visitedValue)) {
          // 若存在
          totalCount = value.visited + visitedValue.toLong;

        } else {
          // 不存在
          totalCount = value.visited
        }

        // 保存数据
        HBaseUtil.putMapData(tableName, rowkey, cfName, Map(
          channelColumnl -> value.channelid,
          visitedColumn -> totalCount
        ))
      }
    })
  }
}

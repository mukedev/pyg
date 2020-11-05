package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 基础算法接口
  */
trait BaseTask[T] {

  // 转换
  def map(clickLogWideDataStream:DataStream[ClickLogWide]): DataStream[T]

  // 分组
  def keyBy(mapDataStream: DataStream[T]):KeyedStream[T, String]

  // 计算窗口
  def timeWindow(keyedDataStream: KeyedStream[T,String]): WindowedStream[T, String, TimeWindow]

  // 聚合
  def reduce(windowDataStream: WindowedStream[T, String, TimeWindow]): DataStream[T]

  // sink HBase
  def sinkToHBase(reducedDataStream: DataStream[T])

  def process(clickLogWideDataStream:DataStream[ClickLogWide]): Unit ={
    val mapDataStream: DataStream[T] = map(clickLogWideDataStream)
    val keyedDataStream: KeyedStream[T, String] = keyBy(mapDataStream)
    val windowDataStream: WindowedStream[T, String, TimeWindow] = timeWindow(keyedDataStream)
    val reducedDataStream: DataStream[T] = reduce(windowDataStream)
    sinkToHBase(reducedDataStream)
  }
}

package com.itheima.realprocess

import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.itheima.realprocess.bean._
import com.itheima.realprocess.task._
import com.itheima.realprocess.util.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

object AdApp {

  def main(args: Array[String]): Unit = {

    // 初始化流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度
    env.setParallelism(1)

    // 添加checkpoint的支持
    // 5s启动一次check
    env.enableCheckpointing(5000)

    // 设置checkpoint 只checkpoint一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // checkpoint最大并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭时，触发额外的checkpoint
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpint-adclick"))

    // flink整合kafka
    val props = new Properties()

    // # Kafka集群地址
    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    // # ZooKeeper集群地址
    props.setProperty("zookeeper.connect", GlobalConfigUtil.zookeeperConnect)
    props.setProperty("topic",GlobalConfigUtil.adInputTopic)
    // # 消费组ID
    props.setProperty("group.id", GlobalConfigUtil.adGroupId)
    // # 自动提交拉取到消费端的消息offset到kafka
    props.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    // # 自动提交offset到zookeeper的时间间隔单位（毫秒）
    props.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    // # 每次消费最新的数据
    props.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](
      GlobalConfigUtil.adInputTopic,
      new SimpleStringSchema(),
      props)

    val kafkaDataStream: DataStream[String] = env.addSource(consumer)

    // JSON -> 元组
    val tupleStreamData = kafkaDataStream.map {
      msgJson =>
        val jsonObject = JSON.parseObject(msgJson)
        val adClickLogStr = jsonObject.getString("message")
        AdClickLog(adClickLogStr)
    }
//    tupleStreamData.print()

    // 添加水印支持
    val watermarkDataStream: DataStream[AdClickLog] = tupleStreamData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[AdClickLog] {
      var currentTimestamp = 0l

      // 延迟时间
      var maxDelayTime = 2000l

      var watermark: Watermark = _

      private val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")

      // 获取当前时间戳
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimestamp - maxDelayTime)
      }

      // 获取事件时间
      override def extractTimestamp(t: AdClickLog, l: Long): Long = {
        val time = format.parse(t.timestamp).getTime
        currentTimestamp = Math.max(time.toLong, l)
        currentTimestamp
      }
    })

    val adClickLogWideDataStream: DataStream[AdClickLogWide] = PreprocessTask.adProcess(watermarkDataStream)

    // 发送数据到kafka中，因为druid后续从kafka中消费数据，所以我们在此进行格式处理，数据转为json字符串，方便druid解析处理
    val adprocess: DataStream[String] = adClickLogWideDataStream.map {
      log =>
        // 隐式转换
        implicit val formats = DefaultFormats
        val str = write(log)
        str
    }

    // sink kafka
    val flinkPro = new FlinkKafkaProducer010[String](
      GlobalConfigUtil.bootstrapServers,
      GlobalConfigUtil.adProcessedTopic,
      new SimpleStringSchema()
    )

    adprocess.addSink(flinkPro)

    env.execute(this.getClass.getName)
  }
}

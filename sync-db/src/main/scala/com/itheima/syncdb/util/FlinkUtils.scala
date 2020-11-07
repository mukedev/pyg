package com.itheima.syncdb.util


import java.util.Properties

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

/**
  * flink统一初始化类
  */
object FlinkUtils {

  def initFlinkStreamEnv = {

    // 获取流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    // 添加Checkpoint的支持
    // 5s钟启动一次checkpoint
    env.enableCheckpointing(5000)
    // 设置checkpoint只checkpoint一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 最大并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭时,触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpoint/"))
    env
  }


  /**
    * Kafka属性设置
    * * @return
    **/
  def getKafkaProperties(): Properties = {

    val properties = new Properties()

    properties.put("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    properties.put("group.id", GlobalConfigUtil.groupId)
    properties.put("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    properties.put("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    // 配置下次重新消费的话，从哪里开始消费
    // latest：从上一次提交的offset位置开始的
    // earlist：从头开始进行（重复消费数据）
    properties.put("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)
    // 配置序列化和反序列化

    properties
  }
}

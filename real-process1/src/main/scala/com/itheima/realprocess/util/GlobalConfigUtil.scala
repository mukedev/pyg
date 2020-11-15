package com.itheima.realprocess.util

import com.typesafe.config.{Config, ConfigFactory}

object GlobalConfigUtil {

  // 通过ConfigFactory load配置
  val config: Config = ConfigFactory.load()

  val bootstrapServers = config.getString("bootstrap.servers")
  val zookeeperConnect = config.getString("zookeeper.connect")
  val inputTopic = config.getString("input.topic")
  val groupId = config.getString("group.id")
  val enableAutoCommit = config.getString("enable.auto.commit")
  val autoCommitIntervalMs = config.getString("auto.commit.interval.ms")
  val autoOffsetReset = config.getString("auto.offset.reset")
  val adGroupId = config.getString("ad.group.id")
  val adInputTopic = config.getString("ad.input.topic")
  val adProcessedTopic = config.getString("ad.processed.topic")


  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(groupId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
    println(adGroupId)
    println(adInputTopic)
  }
}

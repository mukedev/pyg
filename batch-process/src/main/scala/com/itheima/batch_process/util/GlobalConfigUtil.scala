package com.itheima.batch_process.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * 加载配置文件工具类
  */
object GlobalConfigUtil {

  private val config: Config = ConfigFactory.load()

  /*------------------
   * Kafka配置
   *------------------*/

  def bootstrapServers = config.getString("bootstrap.servers")
  def zookeeperConnect = config.getString("zookeeper.connect")
  def inputTopic = config.getString("input.topic")
  def groupId = config.getString("group.id")
  def enableAutoCommit = config.getString("enable.auto.commit")
  def autoCommitIntervalMs = config.getString("auto.commit.interval.ms")
  def autoOffsetReset = config.getString("auto.offset.reset")

  /*
  ** HBase配置
  */

  def hbaseZookeeperQuorum = config.getString("hbase.zookeeper.quorum")
  def hbaseMaster = config.getString("hbase.master")
  def hbaseZookeeperPropertyClientPort = config.getString("hbase.zookeeper.property.clientPort")
  def hbaseRpcTimeout = config.getString("hbase.rpc.timeout")
  def hbaseClientOperatorTimeout = config.getString("hbase.client.operator.timeout")
  def hbaseClientScannerTimeoutPeriod = config.getString("hbase.client.scanner.timeout.period")

  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(groupId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
    println(hbaseZookeeperQuorum)
    println(hbaseMaster)
    println(hbaseZookeeperPropertyClientPort)
    println(hbaseRpcTimeout)
    println(hbaseClientOperatorTimeout)
    println(hbaseClientScannerTimeoutPeriod)
  }
}

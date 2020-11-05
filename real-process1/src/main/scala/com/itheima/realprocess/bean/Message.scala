package com.itheima.realprocess.bean

/**
  * 将消息对象转为样例类
  */
case class Message(
                    var clickLog: ClickLog,
                    var count: Long,
                    var timestamp: Long)



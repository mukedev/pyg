package com.itheima.batch_process.bean

case class PaymethodMoneyCount(
                                date: String,
                                paymethod: String,
                                count: Long,
                                money: Double
                              )

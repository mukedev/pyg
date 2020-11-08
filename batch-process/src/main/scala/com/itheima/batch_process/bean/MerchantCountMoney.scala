package com.itheima.batch_process.bean

case class MerchantCountMoney(
                               var merchantId: String,
                               var date: String,
                               var amount: Double,
                               var count: Long
                             )

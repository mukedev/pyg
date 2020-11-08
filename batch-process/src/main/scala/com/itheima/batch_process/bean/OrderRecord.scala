package com.itheima.batch_process.bean

import com.alibaba.fastjson.JSON

case class OrderRecord(
                        var benefitAmount: Double,// 红包金额
                        var orderAmount: Double,//订单金额
                        var payAmount: Double,//支付金额
                        var activityNum: String,//活动ID
                        var createTime: String,//创建时间
                        var merchantId: String,//商家ID
                        var orderId: String,//订单ID
                        var payTime: String,//支付时间
                        var payMethod: String,//支付方式
                        var voucherAmount: String,//优惠券的金额
                        var commodityId: String,//产品ID
                        var userId: String//用户ID
                      )

object OrderRecord{

  def apply(json:String):OrderRecord={
    JSON.parseObject[OrderRecord](json,classOf[OrderRecord])
  }


  def main(args: Array[String]): Unit = {
    val json = "{\"benefitAmount\":\"20.0\",\"orderAmount\":\"300.0\",\"payAmount\":\"457.0\",\"activityNum\":\"0\",\"createTime\":\"2018-08-13 00:00:06\",\"merchantId\":\"1\",\"orderId\":\"99\",\"payTime\":\"2018-08-13 00:00:06\",\"payMethod\":\"1\",\"voucherAmount\":\"20.0\",\"commodityId\":\"1101\",\"userId\":\"4\"}"

    val record = OrderRecord(json)
    println(OrderRecord(json))
  }
}
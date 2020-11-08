package com.itheima.batch_process.task

import com.itheima.batch_process.bean.{OrderRecord, OrderRecordWide}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
  * 扩展宽表orderRecordWide
  */
object PreProcessTask {

  def process(orderRecordDataSet: DataSet[OrderRecord]) = {

    orderRecordDataSet.map {
      orderRecord => {
        OrderRecordWide(
          orderRecord.benefitAmount.toString,
          orderRecord.orderAmount.toString,
          orderRecord.payAmount.toString,
          orderRecord.activityNum,
          orderRecord.createTime,
          orderRecord.merchantId,
          orderRecord.orderId,
          orderRecord.payTime,
          orderRecord.payMethod,
          orderRecord.voucherAmount,
          orderRecord.commodityId,
          orderRecord.userId,
          fsDateFormat(orderRecord.createTime, "yyyyMMdd"),
          fsDateFormat(orderRecord.createTime, "yyyyMM"),
          fsDateFormat(orderRecord.createTime, "yyyy")
        )
      }
    }

  }

  def fsDateFormat(date: String, pattern: String): String = {
    val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val timestamp: Long = format.parse(date).getTime
    val dateStr: String = FastDateFormat.getInstance(pattern).format(timestamp)
    dateStr
  }


  def main(args: Array[String]): Unit = {
    println(fsDateFormat("2020-10-22 20:22:09","yyyyMMdd"))
    println(fsDateFormat("2020-10-22 20:22:09","yyyyMM"))
    println(fsDateFormat("2020-10-22 20:22:09","yyyy"))
  }
}

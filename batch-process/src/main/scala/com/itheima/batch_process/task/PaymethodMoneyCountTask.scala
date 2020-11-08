package com.itheima.batch_process.task

import com.itheima.batch_process.bean.{OrderRecordWide, PaymethodMoneyCount}
import com.itheima.batch_process.util.HBaseUtil
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object PaymethodMoneyCountTask {

  def process(orderRecordWideDataSet: DataSet[OrderRecordWide]) = {
    // 转换
    val pMCountDataSet: DataSet[PaymethodMoneyCount] = orderRecordWideDataSet.flatMap {
      orderRecordWide => {
        List(
          PaymethodMoneyCount(orderRecordWide.yearMonthDay, orderRecordWide.payMethod, 1, orderRecordWide.payAmount.toDouble),
          PaymethodMoneyCount(orderRecordWide.yearMonth, orderRecordWide.payMethod, 1, orderRecordWide.payAmount.toDouble),
          PaymethodMoneyCount(orderRecordWide.year, orderRecordWide.payMethod, 1, orderRecordWide.payAmount.toDouble))
      }
    }

    pMCountDataSet.print()
    // 分组
    val groupedDataSet = pMCountDataSet.groupBy {
      pMCount => pMCount + pMCount.paymethod
    }

    // 聚合
    val reducedDataSet = groupedDataSet.reduce {
      (t1, t2) => PaymethodMoneyCount(t1.date, t1.paymethod, t1.count + t2.count, t1.money + t1.money)
    }

    // 落地数据到HBase
    reducedDataSet.collect().foreach {
      reduced =>
        // 构建hbase所必须的参数
        val tableName = "analysis_payment"
        val rowkey = reduced.paymethod + ":" + reduced.date
        val cfName = "info"
        val colDateName = "date"
        val colPayMethodName = "payMethod"
        val colCountName = "count"
        val colMoneyName = "money"

        HBaseUtil.putMapData(
          tableName,
          rowkey,
          cfName,
          Map(
            colDateName -> reduced.date,
            colPayMethodName -> reduced.paymethod,
            colCountName -> reduced.count.toString,
            colMoneyName -> reduced.money.toString
          )
        )
    }
  }


}

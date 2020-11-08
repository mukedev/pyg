package com.itheima.batch_process.task

import com.itheima.batch_process.bean.{MerchantCountMoney, OrderRecordWide, PaymethodMoneyCount}
import com.itheima.batch_process.util.HBaseUtil
import org.apache.flink.api.scala.{DataSet, _}

object MerchantCountMoneyTask {

  def process(orderRecordWideDataSet: DataSet[OrderRecordWide]) = {
    // 转换
    val mCMoneyDataSet = orderRecordWideDataSet.flatMap {
      orderRecordWide => {
        List(
          MerchantCountMoney(orderRecordWide.merchantId, orderRecordWide.yearMonthDay, orderRecordWide.payAmount.toDouble, 1),
          MerchantCountMoney(orderRecordWide.merchantId, orderRecordWide.yearMonth, orderRecordWide.payAmount.toDouble, 1),
          MerchantCountMoney(orderRecordWide.merchantId, orderRecordWide.year, orderRecordWide.payAmount.toDouble, 1)
        )
      }
    }

    mCMoneyDataSet.print()
    // 分组
    val groupedDataSet = mCMoneyDataSet.groupBy {
      mCMoney => mCMoney.merchantId + mCMoney.date
    }

    // 聚合
    val reducedDataSet = groupedDataSet.reduce {
      (t1, t2) => MerchantCountMoney(t1.merchantId, t1.date, t1.amount + t2.amount, t1.count + t2.count)
    }

    // 落地数据到HBase
    reducedDataSet.collect().foreach {
      reduced =>
        // 构建hbase所必须的参数
        val tableName = "analysis_merchant"
        val rowkey = reduced.merchantId + ":" + reduced.date
        val cfName = "info"
        val colPayMethodName = "merchantId"
        val colDateName = "date"
        val colMoneyName = "amount"
        val colCountName = "count"

        HBaseUtil.putMapData(
          tableName,
          rowkey,
          cfName,
          Map(
            colPayMethodName -> reduced.merchantId,
            colDateName -> reduced.date,
            colMoneyName -> reduced.amount.toString,
            colCountName -> reduced.count.toString
          )
        )
    }
  }


}

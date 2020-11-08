package com.itheima.batch_process

import com.itheima.batch_process.bean.{OrderRecord, OrderRecordWide}
import com.itheima.batch_process.task.{MerchantCountMoneyTask, PaymethodMoneyCountTask, PreProcessTask}
import com.itheima.batch_process.util.HBaseTableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

object App {

  def main(args: Array[String]): Unit = {

    // 加载批处理环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 构建dataset
    val tupleDataset: DataSet[tuple.Tuple2[String, String]] = env.createInput(new HBaseTableInputFormat("mysql.pyg.orderRecord"))

    val orderRecordDataSet: DataSet[OrderRecord] = tupleDataset.map {
      tuple =>
        OrderRecord(tuple.f1)
    }

    val orderRecordWideDataSet: DataSet[OrderRecordWide] = PreProcessTask.process(orderRecordDataSet)

    orderRecordWideDataSet.print()

    PaymethodMoneyCountTask.process(orderRecordWideDataSet)

    MerchantCountMoneyTask.process(orderRecordWideDataSet)

  }
}

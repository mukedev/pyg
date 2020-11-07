package com.itheima.syncdb.task

import java.util

import com.alibaba.fastjson.JSON
import com.itheima.syncdb.bean.{Canal, ColumnValuePair, HBaseOperation}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._
import scala.collection.mutable

object PreprocessTask {

  def process(watermarkDataStream: DataStream[Canal]) = {

    watermarkDataStream.flatMap {
      canal => {
        // 转换json串为集合
        val pairs: util.List[ColumnValuePair] = JSON.parseArray[ColumnValuePair](canal.columnValueList, classOf[ColumnValuePair])
        val colValueList: mutable.Buffer[ColumnValuePair] = pairs.asScala

        val opType = canal.eventType

        val tableName = s"mysql.${canal.dbName}.${canal.tableName}"
        val cfName = "info"
        val rowkey = colValueList(0).columnValue

        opType match {
          case "INSERT" =>
            // 如果是INSERT操作，将每一个列值对转换为一个HBaseOperation
            colValueList.map {
              colVal =>
                HBaseOperation(opType, tableName, cfName, rowkey, colVal.columnName, colVal.columnValue)
            }
          case "UPDATE" =>
            // UPDATE操作 -> 过滤出来isValid字段为true的列，再转换为HBaseOperation
            colValueList.filter(_.isValid).map {
              colVal =>
                HBaseOperation(opType, tableName, cfName, rowkey, colVal.columnName, colVal.columnValue)
            }
          case "DELETE" =>
            // DELETE操作 -> 只生成一条DELETE的HBaseOperation的List
            List(HBaseOperation(opType, tableName, cfName, rowkey, "", ""))
        }

      }
    }
  }
}

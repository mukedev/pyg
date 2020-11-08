package com.itheima.batch_process.util

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}

/**
  * 这是Flink整合HBase的工具类,去读取HBase的表的数据
  *
  * 1. 继承TableInputFormat<T extends Tuple>, 需要指定泛型
  * 2. 泛型是Java的Tuple : org.apache.flink.api.java.tuple
  * 3. Tuple2 中第一个元素装载rowkey 第二个元素装 列名列值的JSON字符串
  * 4. 实现抽象方法
  * getScanner: 返回Scan对象, 父类中已经定义了scan ,我们在本方法中需要为父类的scan赋值
  * getTableName : 返回表名,我们可以在类的构造方法中传递表名
  * mapResultToTuple : 转换HBase中取到的Result进行转换为Tuple
  *
  *  a. 取rowkey
  *  b. 取cell数组
  *  c. 遍历数组,取列名和列值
  *  d. 构造JSONObject
  *  e. 构造Tuple2
  */
class HBaseTableInputFormat(var tableName: String) extends ItheimaTableInputFormat[Tuple2[String, String]] {

  //  返回Scan对象
  override def getScanner: Scan = {
    scan = new Scan()
    scan
  }

  //返回表名
  override def getTableName: String = {
    tableName
  }

  //转换HBase中取到的Result进行转换为Tuple
  override def mapResultToTuple(result: Result): Tuple2[String, String] = {

    // 取rowkey
    val rowkey: String = Bytes.toString(result.getRow)
    // 取cells
    val cells: Array[Cell] = result.rawCells()
    // 定义JSONOBject
    val jsonObject = new JSONObject()
    //遍历cells
    for (i <- 0 until cells.length) {
      val colName: String = Bytes.toString(CellUtil.cloneQualifier(cells(i)))
      val colValue: String = Bytes.toString(CellUtil.cloneValue(cells(i)))

      jsonObject.put(colName, colValue)
    }

    new Tuple2[String, String](rowkey, jsonObject.toString)

  }
}

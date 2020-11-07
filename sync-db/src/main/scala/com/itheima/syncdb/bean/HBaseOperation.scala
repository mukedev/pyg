package com.itheima.syncdb.bean

/** HBase操作样例类
  * 操作类型（opType） = INSERT/DELETE/UPDATE
  * 表名（tableName）= binlog数据库名_binlog表名
  * 列蔟名（cfName）= 固定为info
  * rowkey = 唯一主键（取binlog中列数据的第一个）
  * 列名（colName）= binlog中列名
  * 列值（colValue）= binlog中列值
  */
case class HBaseOperation(
                           var opType: String,
                           var tableName: String,
                           var cfName: String,
                           var rowkey: String,
                           var colName: String,
                           var colValue: String
                         )
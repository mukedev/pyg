package com.itheima.syncdb.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes


/**
  * Hbase工具类
  *
  * 创建/获取表
  * 插入/更新一列数据
  * 插入/更新多个列数据
  * 根据rowkey，列族+列名获取单列数据
  * 根据rowkey，列族+列名集合获取多列数据
  * 根据rowkey删除一条数据
  *
  *
  *
  */
object HBaseUtil {


  // 读取配置文件
  private val configuration: Configuration = HBaseConfiguration.create()
  // 创建连接
  private val connection: Connection = ConnectionFactory.createConnection(configuration)
  // 创建HBase管理类
  private val admin: Admin = connection.getAdmin


  /**
    * 创建/获取Table类
    * @param tableNameStr       表名
    * @param columnFamilyName   列族名
    * @return                   Table对象
    */
  def getTable(tableNameStr:String,columnFamilyName:String):Table = {

    // 获取TableName
    val tableName: TableName = TableName.valueOf(tableNameStr)
    // 判断表是否可用
    if (!admin.tableExists(tableName)){
      // 构建TableDescriptorBuilder
      val tableDescriptor: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)

      val familyDescriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes()).build()

      // 给表添加列族
      tableDescriptor.setColumnFamily(familyDescriptor)

      //创建表
      admin.createTable(tableDescriptor.build())

    }
    connection.getTable(tableName)
  }

  /**
    * 插入/更新一列数据
    * @param tableNameStr 表名
    * @param rowkey       rowkey
    * @param columnFamily 列族名
    * @param column       列族字段
    * @param data         列族字段值
    */
  def putData(tableNameStr:String, rowkey:String, columnFamily:String, column:String, data:String):Unit = {

    val table: Table = getTable(tableNameStr, columnFamily)

    try {
      val put = new Put(rowkey.getBytes())

      put.addColumn(columnFamily.getBytes(), column.getBytes(), data.getBytes())
      table.put(put)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      table.close()
    }
  }


  /**
    * 插入/更新多列值
    * @param tableNameStr
    * @param rowkey
    * @param columnFamily
    * @param mapData
    */
  def putMapData(tableNameStr:String, rowkey:String, columnFamily:String, mapData:Map[String,Any]): Unit = {

    val table: Table = getTable(tableNameStr,columnFamily)

    try {
      val put = new Put(rowkey.getBytes())

      for ((key, value) <- mapData) {
        put.addColumn(columnFamily.getBytes(), key.getBytes(), value.toString.getBytes())
      }

      table.put(put)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
    * 读取单列数据
    * @param tableNameStr 表名
    * @param rowkey       rowkey
    * @param columnFamily 列族名
    * @param column       列族字段
    * @return
    */
  def getData(tableNameStr:String, rowkey:String, columnFamily:String, column:String): String = {
    val table: Table = getTable(tableNameStr, columnFamily)

    try {
      val get = new Get(rowkey.getBytes())

      get.addColumn(columnFamily.getBytes(), column.getBytes())

      val result: Result = table.get(get)

      if (result != null && result.containsColumn(columnFamily.getBytes(), column.getBytes())){

        val bytes: Array[Byte] = result.getValue(columnFamily.getBytes(), column.getBytes())

        Bytes.toString(bytes)
      } else {
        ""
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        ""
    } finally {
      table.close()
    }
  }

  /**
    * 读取多列数据
    * @param tableNameStr   表名
    * @param rowkey         rowkey
    * @param columnFamily   列族名
    * @param columnList     列字段集合
    * @return
    */
  def getMapData(tableNameStr:String, rowkey:String, columnFamily:String, columnList:List[String]): Map[String, Any] = {
    val table: Table = getTable(tableNameStr, columnFamily)

    try {
      val get = new Get(rowkey.getBytes())

      val result = table.get(get)

      columnList.map {
        col =>
          val bytes: Array[Byte] = result.getValue(columnFamily.getBytes(), col.getBytes())

          if (bytes != null && bytes.size > 0) {
            col -> Bytes.toString(bytes)
          } else {
            "" -> ""
          }
      }.filter(_._1 != "").toMap

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        Map[String, String]()
    } finally {
      table.close()
    }
  }


  /**
    * 刪除列族
    * @param tableNameStr   表名
    * @param rowkey         rowkey
    * @param columnFamily   列族名
    */
  def deleteData(tableNameStr:String, rowkey:String, columnFamily:String): Unit = {
    val table: Table = getTable(tableNameStr, columnFamily)

    try {
      val delete = new Delete(rowkey.getBytes())
      table.delete(delete)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      table.close()
    }
  }


  def main(args: Array[String]): Unit = {
//    getTable("test","info")

//    putData("test","1","info","t1","Hello word")

//    println(getData("test","1","info","t1"))

//    val map = Map(
//      "t2" -> "bigData",
//      "t3" -> "flink",
//      "t4" -> "hive"
//    )
//    putMapData("test","1","info",map)

     deleteData("test","1","info")

     println(getMapData("test","1","info",List("t1","t2")))


  }
}

package com.itheima.syncdb.bean

case class ColumnValuePair(
                            var columnName: String,
                            var columnValue: String,
                            var isValid: Boolean
                          )

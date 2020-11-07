import java.util.Properties

import com.itheima.syncdb.bean.{Canal, HBaseOperation}
import com.itheima.syncdb.task.PreprocessTask
import com.itheima.syncdb.util.{FlinkUtils, GlobalConfigUtil, HBaseUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object App {

  def main(args: Array[String]): Unit = {

    // Flink流式环境创建
    val env: StreamExecutionEnvironment = FlinkUtils.initFlinkStreamEnv

    // 整合kafka的数据
    val props: Properties = FlinkUtils.getKafkaProperties()

    val consumer = new FlinkKafkaConsumer010[String](GlobalConfigUtil.inputTopic, new SimpleStringSchema(), props)

    val kafkaDataStream: DataStream[String] = env.addSource(consumer)

    kafkaDataStream.print()

    val canalDataStream: DataStream[Canal] = kafkaDataStream.map {
      json =>
        Canal(json)
    }

    // 添加水印
    val watermarkDataStream: DataStream[Canal] = canalDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Canal] {

      var currentTimeStamp = 0l
      val maxDelayTime = 2000

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStamp - maxDelayTime)
      }

      override def extractTimestamp(t: Canal, l: Long): Long = {
        currentTimeStamp = Math.max(t.timestamp, l)
        currentTimeStamp
      }
    })

    val hBaseDataStream: DataStream[HBaseOperation] = PreprocessTask.process(watermarkDataStream)

    hBaseDataStream.addSink(new SinkFunction[HBaseOperation] {
      override def invoke(value: HBaseOperation, context: SinkFunction.Context[_]): Unit = {
        value.opType match {
          case "DELETE" =>
            HBaseUtil.deleteData(value.tableName, value.rowkey, value.cfName)
          case _ =>
            HBaseUtil.putData(value.tableName, value.rowkey, value.cfName, value.colName, value.colValue)
        }
      }
    })

    env.execute(this.getClass.getName)
  }

}

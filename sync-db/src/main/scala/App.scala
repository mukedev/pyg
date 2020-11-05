import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object App {

  def main(args: Array[String]): Unit = {

    // Flink流式环境创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置窗口
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 并行度
    env.setParallelism(1)

    // 设置checkpoint
    // 开启checkpoint,间隔时间为5s
    env.enableCheckpointing(5000)
    // 设置checkpoint处理方式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 设置并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置检查点存储位置
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpoint"))

    val testDs: DataStream[Int] = env.fromCollection(List(1,2,3,4))

    testDs.print()

    env.execute(this.getClass.getName)
  }

}

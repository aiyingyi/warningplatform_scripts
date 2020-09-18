package adc.offline

import java.util.Properties

import adc.{ShellRPC, ShellUtil}
import ch.ethz.ssh2.Connection
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object FlinkTest {

  // 从kafka中获取充电信息
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    env.setParallelism(10)

    //  设置状态后端与检查点
    env.setStateBackend(new MemoryStateBackend())
    // 触发检查点的间隔，周期性启动检查点，单位ms
    env.enableCheckpointing(1000L)
    //设置状态一致性级别
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L))


    // 配置kafka属性
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "kafka35:9092")
    properties.setProperty("group.id", "consumer-group9")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    // 创建数据源
    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("chargeNotification", new SimpleStringSchema(), properties))

    // 将消息转换成样例类
    val notation: DataStream[ChargeNotation] = dataStream
      .map(data => JSON.parseObject[ChargeNotation](data, classOf[ChargeNotation]))
    /*  .map(new RichMapFunction[String, ChargeNotation] {

        override def map(value: String): ChargeNotation = {
          JSON.parseObject[ChargeNotation](value, classOf[ChargeNotation])
        }
      })*/

    // 按照vin进行keyBy
    notation.keyBy(_.vin).addSink(new RichSinkFunction[ChargeNotation] {

      var conn: Connection = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = ShellUtil.getConnection("root", "Rfh9mZDxerOqdM4V", "192.168.11.32", 12816)
      }

      // 执行脚本
      override def invoke(chargeNotation: ChargeNotation, context: SinkFunction.Context[_]): Unit = {

        println("------" + getRuntimeContext.getIndexOfThisSubtask)
        ShellUtil.exec(conn, "/opt/software/a.sh " + chargeNotation.startMsgTime + " " + chargeNotation.endMsgTime + " " + chargeNotation.vin)
      }

      // 关闭连接
      override def close(): Unit = {
        super.close()
        conn.close()
      }


    })
    // 执行任务
    env.execute("charge_mode_electricity_frequency")

  }

}

case class ChargeNotation(val endMsgTime: String, val startMsgTime: String, val vin: String)



package adc.offline

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object SQLTest {

  def main(args: Array[String]): Unit = {

    // 创建使用Blink-Planner的流式执行环境
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv = TableEnvironment.create(bbSettings)


  }
}

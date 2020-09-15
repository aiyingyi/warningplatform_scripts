package adc.hive

import org.apache.spark.sql.SparkSession

object SCTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("SQLTest")
      .getOrCreate()
    spark.sql("show tables").show()
    //释放资源
    spark.stop()


  }

}

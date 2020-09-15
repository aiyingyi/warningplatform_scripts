package adc.hive;
import org.apache.spark.sql.SparkSession;
public class Test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .enableHiveSupport()
                .master("local[*]")
                .appName("SQLTest")
                .getOrCreate();
        spark.sql("show databases").show();
        //释放资源
        spark.stop();
    }
}

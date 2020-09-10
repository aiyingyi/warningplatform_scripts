package adc.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

import java.util.ArrayList;


public class Particle extends UDAF {

    public Particle() {
        super();
    }

    public static class MyUDAF implements UDAFEvaluator {

        //最终结果
        private DoubleWritable result;

        private ArrayList<DoubleWritable> data;

        //负责初始化计算函数并设置它的内部状态，result是存放最终结果的
        public void init() {
            result = null;
            data = new ArrayList<DoubleWritable>();
        }

        //每次对一个新值进行聚集计算都会调用iterate方法
        public boolean iterate(DoubleWritable value) {
            if (value == null)
                return false;
            else
                data.add(value);
            return true;
        }

        //Hive需要部分聚集结果的时候会调用该方法
        //会返回一个封装了聚集计算当前状态的对象
        public DoubleWritable terminatePartial() {
            return null;
        }

        //合并两个部分聚集值会调用这个方法
        public boolean merge(DoubleWritable other) {
            return iterate(other);
        }

        //Hive需要最终聚集结果时候会调用该方法
        public DoubleWritable terminate() {
            return result;
        }
    }


}

package adc.hive;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;


public class CellVolFrequencyUDAF extends UDAF {

    public static class FrequencyUDAF implements UDAFEvaluator {

        private LongWritable result[] = new LongWritable[96];
        @Override
        public void init() {

        }

        //每次对一个新值进行聚集计算都会调用iterate方法
        public boolean iterate(String value)
        {
            if(value==null)
                return false;
            else{
                String[] terms = value.split(",");
                int index = Integer.parseInt(terms[0]);
                long total = Long.parseLong(terms[1]);
                result[index-1].set(result[index-1].get() +total);
            }
            return true;
        }

        //Hive需要部分聚集结果的时候会调用该方法
        //会返回一个封装了聚集计算当前状态的对象
        public LongWritable[] terminatePartial()
        {
            return result;
        }
        //合并两个部分聚集值会调用这个方法
        public boolean merge(LongWritable other[])
        {
            return mergePar(other);
        }
        private boolean mergePar(LongWritable[] other) {

            if(other==null)
                return false;
            else{
                for(int i = 0;i<result.length;i++){
                    result[i].set(result[i].get()+other[i].get());
                }
            }
            return true;
        }
        //Hive需要最终聚集结果时候会调用该方法
        public LongWritable[] terminate()
        {
            return result;
        }

    }


}

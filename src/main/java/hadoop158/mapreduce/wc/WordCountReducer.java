package hadoop158.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.定义一个计数器
        int sum = 0;
        //2.累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        //3.写出
        context.write(key, new IntWritable(sum));
    }
}

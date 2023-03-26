package hadoop158.mapreduce.sales;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class SaleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text v1, Context context) throws java.io.IOException, InterruptedException {
        String data = v1.toString();
        //分词
        String[] words = data.split(",");
        //输出 k2 v2
//        context.write(new Text(words[0]), new DoubleWritable(Double.parseDouble(words[2])));
        for (String k2 : words) {
            context.write(new Text(k2), new DoubleWritable(1)); // 1 is the value of the key k2
        }
    }
}

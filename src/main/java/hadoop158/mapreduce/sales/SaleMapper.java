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
        String date[] = words[2].split("-");//因为表格中的日期，获取之后是一个1998-01-10的字符串，需要再次分割
        String year = date[0];//将分割完的字符串的年份取出来

        context.write(new Text(year), new DoubleWritable(Double.parseDouble(words[6])));
    }
}

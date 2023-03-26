package hadoop158.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息以及封装任务
        //2.设置输入的路径
        //3.设置map相关参数
        //4.设置reduce相关参数
        //5.设置输出的路径
        //6.提交
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int argLen = otherArgs.length;
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //获取一个作业实例，名称为Word Count
        Job job = Job.getInstance(conf, "Word Count");
        //设置运行的jar包里的类
        job.setJarByClass(WordCountMain.class);
        //设置Mapper
        job.setMapperClass(WordCountMapper.class);
        //设置Reducer
        job.setReducerClass(WordCountReducer.class);
        //设置Combiner
        job.setCombinerClass(WordCountReducer.class);
        //设置k4类型
        job.setOutputKeyClass(Text.class);
        //设置v4类型
        job.setOutputValueClass(IntWritable.class);
        //可以接受多个参数作为输入文件路径
        for (int i = 0; i < argLen - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //最后的一个参数是结果输出路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[argLen - 1]));
        //如果作业运行成功就成功退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

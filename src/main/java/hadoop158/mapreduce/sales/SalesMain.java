package hadoop158.mapreduce.sales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SalesMain {
    //1.获取配置信息
    //2.获取Job对象
    //3.设置jar存储位置
    //4.关联Map和Reduce类
    //5.设置Mapper阶段输出数据的key和value类型
    //6.设置最终数据输出的key和value类型
    //7.设置输入路径和输出路径
    //8.提交job
    public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://192.169.128.11:8020");
//        conf.set("yarn.resourcemanager.hostname", "nodea158");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int argLen = otherArgs.length;
        if (argLen != 2) {
            System.err.println("Usage: xxx.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        //获取一个作业实例
        Job job = Job.getInstance(conf, "Sales Statistics");
        //设置运行jar包里的类
        job.setJarByClass(SalesMain.class);
        //设置Mapper类
        job.setMapperClass(SaleMapper.class);
        //设置Reducer类
        job.setReducerClass(SaleReducer.class);
        //设置Combiner类
        job.setCombinerClass(SaleReducer.class);
        //设置k4的类型
        job.setMapOutputKeyClass(Text.class);
        //设置v4的类型
        job.setMapOutputValueClass(DoubleWritable.class);
        //可以接收多个参数是结果输出路径
        for (int i = 0; i < argLen - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //最后的一个参数是结果输出路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[argLen - 1]));
        //提交作业成功退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

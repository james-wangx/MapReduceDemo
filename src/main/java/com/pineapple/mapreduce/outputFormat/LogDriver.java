package com.pineapple.mapreduce.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义的 OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        Path inputPath = new Path("input/outputFormat");
        Path outputPath = new Path("output/outputFormat");

        FileInputFormat.setInputPaths(job, inputPath);
        // 虽然我们自定义了 OutputFormat ， 但是因为我们的 OutputFormat 继承自 FileOutputFormat
        // 而 FileOutputFormat 要输出一个 _SUCCESS 文件，所以在这还需要指定一个输出目录
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

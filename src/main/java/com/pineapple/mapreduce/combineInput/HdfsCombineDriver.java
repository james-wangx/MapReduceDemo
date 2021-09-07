package com.pineapple.mapreduce.combineInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HdfsCombineDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HdfsCombineDriver.class);
        job.setJobName("CombineInput");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);

        // 虚拟存储切片最大值设置 4m
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 虚拟存储切片最大值设置 20m
        CombineTextInputFormat.setMaxInputSplitSize(job, Long.parseLong(args[0]) * 1024 * 1024);

        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

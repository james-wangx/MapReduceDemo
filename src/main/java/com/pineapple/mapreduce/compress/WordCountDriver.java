package com.pineapple.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        // 开启 Map 端输出压缩
        conf.setBoolean("mapreduce.map.output.compass", true);

        // 设置 Map 端输出压缩方式
        conf.setClass("mapreduce.map.output.compass.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);
        job.setJobName("WordCount");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath = new Path("output/wordcount");
        if (fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        FileInputFormat.setInputPaths(job, new Path("input/wordcount"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 设置 Reduce 端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩方式
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

package com.pineapple.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HdfsPartitionDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HdfsPartitionDriver.class);
        job.setJobName("ProvincePartitioner");

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        // 设置分区类
        job.setPartitionerClass(ProvincePartitioner.class);

        // 设置分区数
        job.setNumReduceTasks(5);

        Path inputPtah = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        FileInputFormat.setInputPaths(job, inputPtah);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

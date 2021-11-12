package com.pineapple.mapreduce.serialization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SerializationDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SerializationDriver.class);
        job.setJobName("Serialization");

        job.setMapperClass(SerializationMapper.class);
        job.setReducerClass(SerializationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SerializationBean.class);

        job.setOutputKeyClass(SerializationBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/serialization"));
        FileOutputFormat.setOutputPath(job, new Path("output/serialization"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

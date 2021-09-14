package com.pineapple.mapreduce.etl;

import com.pineapple.mapreduce.outputFormat.LogDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WebLogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"input/log", "output/log"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LogDriver.class);
        job.setJobName("WebLog");

        job.setMapperClass(WebLogMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 无需 Reduce
        job.setNumReduceTasks(0);

        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        final FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

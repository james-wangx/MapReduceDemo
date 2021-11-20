package com.pineapple.mapreduce.countRepeat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("CountRepeat");
        job.setJarByClass(CountDriver.class);

        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath = new Path("input/countRepeat");
        Path outputPath = new Path("output/outputRepeat");

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        Counter repeatLineCounter = job.getCounters().findCounter(RepeatLineCounter.REPEAT_LINE_NUMS);
        System.out.println("Repeat line nums: " + repeatLineCounter.getValue());

        System.exit(result ? 0 : 1);
    }
}

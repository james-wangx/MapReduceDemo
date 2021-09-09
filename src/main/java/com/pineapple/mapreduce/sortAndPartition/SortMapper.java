package com.pineapple.mapreduce.sortAndPartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, Text> {

    private final SortBean outK = new SortBean();
    private final Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, SortBean, Text>.Context context)
            throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split("\t");

        outV.set(split[0]);
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow(Long.parseLong(split[3]));

        context.write(outK, outV);
    }
}

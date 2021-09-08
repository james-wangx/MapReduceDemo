package com.pineapple.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {

    private final Text outK = new Text();
    private final PartitionBean outV = new PartitionBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PartitionBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        outK.set(split[1]);
        // 封装 FlowBean
        outV.setUpFlow(Long.parseLong(split[split.length - 3]));
        outV.setDownFlow(Long.parseLong(split[split.length - 2]));
        // 可以到 Reducer 再计算总流量
//        outK.setSumFlow();

        context.write(outK, outV);
    }
}

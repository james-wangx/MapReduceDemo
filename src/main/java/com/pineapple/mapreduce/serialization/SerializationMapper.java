package com.pineapple.mapreduce.serialization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SerializationMapper extends Mapper<LongWritable, Text, Text, SerializationBean> {

    private final Text outK = new Text();
    private final SerializationBean bean = new SerializationBean();

    @Override
    protected void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, Text, SerializationBean>.Context context)
            throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split("\t");

        outK.set(split[1]);
        bean.setUpFlow(Integer.parseInt(split[split.length - 3]));
        bean.setDownFlow(Integer.parseInt(split[split.length - 2]));

        context.write(outK, bean);
    }
}

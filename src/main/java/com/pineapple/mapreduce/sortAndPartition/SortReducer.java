package com.pineapple.mapreduce.sortAndPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, Text, Text, SortBean> {

    @Override
    protected void reduce(SortBean key, Iterable<Text> values, Reducer<SortBean, Text, Text, SortBean>.Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}

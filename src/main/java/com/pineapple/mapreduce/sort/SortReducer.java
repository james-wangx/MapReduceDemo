package com.pineapple.mapreduce.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, Text, Text, SortBean> {

    @Override
    protected void reduce(SortBean key, Iterable<Text> values, Reducer<SortBean, Text, Text, SortBean>.Context context)
            throws IOException, InterruptedException {
        // 根据 SortBean 中定义的比较方法，有可能产生相同的 Key
        for (Text value : values) {
            context.write(value, key);
        }
    }
}

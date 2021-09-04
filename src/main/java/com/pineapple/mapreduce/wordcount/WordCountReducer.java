package com.pineapple.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce 阶段的输入数据类型要和 Map 阶段的输出数据类型对应
 * KEYIN, reduce阶段输入的key的类型：Text
 * VALUEIN, reduce阶段输入value类型：IntWritable
 * KEYOUT, reduce阶段输出key类型：Text
 * VALUEOUT, reduce阶段输出value类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable outV = new IntWritable();

    /**
     * 每一个 Key 都会调用此方法
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context
            context) throws IOException, InterruptedException {

        int sum = 0;

        // 值 values 是一个迭代器，可以使用 for each 循环取出每一项
        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);

        // 写入文件
        context.write(key, outV);
    }
}

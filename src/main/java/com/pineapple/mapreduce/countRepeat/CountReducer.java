package com.pineapple.mapreduce.countRepeat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>
            .Context context) throws IOException, InterruptedException {
        int length = 0;

        for (NullWritable value : values)
            length++;

        if (length >= 2)
                context.getCounter(RepeatLineCounter.REPEAT_LINE_NUMS).increment(length - 1);

        context.write(key, NullWritable.get());
    }
}

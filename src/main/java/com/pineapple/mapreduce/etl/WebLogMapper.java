package com.pineapple.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // 58.215.204.118 - - [18/Sep/2013:06:51:41 +0000] "-" 400 0 "-" "-"
        final String line = value.toString();

        boolean result = parseLong(line, context);

        if (!result)
            return;

        context.write(value, NullWritable.get());
    }

    private boolean parseLong(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        final String[] fields = line.split(" ");

        // 判断日志长度是否大于 11
        return fields.length > 11;
    }
}

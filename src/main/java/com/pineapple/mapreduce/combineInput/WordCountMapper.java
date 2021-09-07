package com.pineapple.mapreduce.combineInput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outK = new Text();
    private final IntWritable outV = new IntWritable(1);
    private final String regex = ".*?(\\w+-?\\w+).*?";
    private Pattern pattern;
    private Matcher matcher;

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {
            // 过滤掉标点符号（不包括'-'）
            pattern = Pattern.compile(regex);
            matcher = pattern.matcher(word);
            if (matcher.matches()) {
                String group = matcher.group(1);
                outK.set(group.toLowerCase());
                context.write(outK, outV);
            }
        }
    }
}

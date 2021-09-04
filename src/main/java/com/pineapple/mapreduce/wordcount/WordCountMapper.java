package com.pineapple.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Map 阶段的输入和输出数据都是 KV 键值对的形式，KV 的类型可以自定义
 * KEYIN：map阶段输入的key的类型：LongWritable
 * VALUEIN：map阶段输入value类型：Text
 * KEYOUT, map阶段输出key类型：Text
 * VALUEOUT, map阶段输出value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outK = new Text();
    private final IntWritable outV = new IntWritable(1);
    private final String regex = ".*?(\\w+-?\\w+).*?";
    private Pattern pattern;

    /**
     * 重写 map 方法，每个 KV 键值对都会调用一次这个方法
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // value 为这一行的数据，Text 类型可以转换成 Java 中的 String 类型以获取更多有关字符串的操作
        String line = value.toString();

        // 按照空格切割字符串
        String[] words = line.split(" ");

        // 循环写入环形缓冲区
        for (String word : words) {
            pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(word);
            if (matcher.matches()) {
                String group = matcher.group(1);
                // 封装outK，即将 String 转小写再转为 Text 类型
                outK.set(group.toLowerCase());
                // 写入，参数类型和输出的 KV 类型一致
                context.write(outK, outV);
            }
        }
    }
}

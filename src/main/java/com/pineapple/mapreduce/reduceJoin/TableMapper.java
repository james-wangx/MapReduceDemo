package com.pineapple.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;
    private final Text outK = new Text();
    private final TableBean outV = new TableBean();

    /**
     * 重写初始化方法，获取文件名称
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context){
        // 获取切片信息，InputSplit 是抽象方法，具体用 FileSplit
        FileSplit split = (FileSplit) context.getInputSplit();
        // 获取文件名
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        // 判断是哪个文件的
        if (fileName.contains("order")) { // 订单表
            String[] split = line.split("\t");
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setpId(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPName("");
            outV.setFlag("order");
        } else { // 商品表
            String[] split = line.split("\t");
            outK.set(split[0]);
            outV.setId("");
            outV.setpId(split[0]);
            outV.setAmount(0);
            outV.setPName(split[1]);
            outV.setFlag("pd");
        }

        context.write(outK, outV);
    }
}

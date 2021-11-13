package com.pineapple.mapreduce.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartition extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text key, NullWritable value, int num) {

        String string = key.toString();
        String[] split = string.split("\t");
        String phone = split[1].substring(0, 3);

        switch (phone) {
        case "136":
            return 0;
        case "137":
            return 1;
        case "138":
            return 2;
        case "139":
            return 3;
        default:
            return 4;
        }
    }
}

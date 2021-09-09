package com.pineapple.mapreduce.sortAndPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner extends Partitioner<SortBean, Text> {

    @Override
    public int getPartition(SortBean sortBean, Text text, int numPartitions) {
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        switch (prePhone) {
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

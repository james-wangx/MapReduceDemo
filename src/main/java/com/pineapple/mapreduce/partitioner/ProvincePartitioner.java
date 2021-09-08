package com.pineapple.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, PartitionBean> {

    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int numPartitions) {
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

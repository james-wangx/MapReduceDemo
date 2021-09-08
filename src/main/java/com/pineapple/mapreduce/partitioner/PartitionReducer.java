package com.pineapple.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionReducer extends Reducer<Text, PartitionBean, Text, PartitionBean> {

    private final PartitionBean outV = new PartitionBean();

    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Reducer<Text, PartitionBean, Text, PartitionBean>.Context context)
            throws IOException, InterruptedException {

        long totalUp = 0;
        long totalDown = 0;

        for (PartitionBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        // 封装 FlowBean
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        context.write(key, outV);
    }
}

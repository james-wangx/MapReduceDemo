package com.pineapple.mapreduce.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 如果自定义类型作为 Key 输入环形缓冲区，那么它必须支持排序
 * MR 提供了 WritableComparable 接口可以在序列化的同时支持排序
 * implements Writable, Comparable<T> 的方式是不允许的
 */
public class SortBean implements WritableComparable<SortBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public SortBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(SortBean o) {
        // 先按照总流量的倒序排序
        if (this.sumFlow > o.sumFlow)
            return -1;
        else if (this.sumFlow < o.sumFlow)
            return 1;
        else {
            // 再按照上行流量的正序排序
            return Long.compare(this.upFlow, o.upFlow);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}

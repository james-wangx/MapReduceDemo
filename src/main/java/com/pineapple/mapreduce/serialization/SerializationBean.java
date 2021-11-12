package com.pineapple.mapreduce.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SerializationBean implements Writable {

    private String phone;
    private int upFlow;
    private int downFlow;
    private int sumFlow;

    public SerializationBean() {
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.phone);
        out.writeInt(this.upFlow);
        out.writeInt(this.downFlow);
        out.writeInt(this.sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.sumFlow = in.readInt();
    }

    @Override
    public String toString() {
        return this.phone + "\t" + this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}

package h2p2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

public class PRNodeWritable implements Writable {
    private int node;
    private double value;

    public PRNodeWritable(int node, double value) {
        this.node = node;
        this.value = value;
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(node);
        out.writeDouble(value);
    }
    public void readFields(DataInput in) throws IOException {
        node = in.readInt();
        value = in.readDouble();
    }
    public int getNode() {
        return this.node;
    }
    public double getValue() {
        return this.value;
    }
}


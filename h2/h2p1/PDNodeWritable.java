package h2p1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

public class PDNodeWritable implements Writable {
    private int node;
    private int dist;

    public PDNodeWritable(int node, int dist) {
        this.node = node;
        this.dist = dist;
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(node);
        out.writeInt(dist);
    }
    public void readFields(DataInput in) throws IOException {
        node = in.readInt();
        dist = in.readInt();
    }
    public int getNode() {
        return this.node;
    }
    public int getDist() {
        return this.dist;
    }
}

class Hash implements Serializable {
    private HashMap<Key, Integer> hash = new HashMap<Key, Integer>();
    private static class Key implements Serializable {
        public int source;
        public int destination;

        public Key(int source, int destination) {
            this.source = source;
            this.destination = destination;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Key)) {
                return false;
            }
            Key that = (Key) obj;
            return this.source == that.source && this.destination == that.destination;
        }

        @Override
        public int hashCode() {
            return (source << 16) + destination;
        }
    }

    public Hash() {

    }
    public Integer get(int src, int dest) {
        return hash.get(new Key(src, dest));
    }
    public void put(int src, int dest, int w) {
        hash.put(new Key(src, dest), w);
    }
    public boolean containsKey(int src, int dest) {
        return hash.containsKey(new Key(src, dest));
    }
}

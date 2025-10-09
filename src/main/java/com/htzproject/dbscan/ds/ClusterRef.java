package com.htzproject.dbscan.ds;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterRef implements WritableComparable<ClusterRef>, Cloneable {
    private int pid;
    private int localClusterId;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(pid);
        out.writeInt(localClusterId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.pid = in.readInt();
        this.localClusterId = in.readInt();
    }

    @Override
    public int compareTo(ClusterRef o) {
        int c = Integer.compare(pid, o.pid);
        return (c != 0) ? c : Integer.compare(localClusterId, o.localClusterId);
    }

    @Override
    public String toString() {
        return pid + "_" + localClusterId;
    }

    public static ClusterRef parse(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("Invalid ClusterRef string: " + str);
        }

        str = str.trim();
        String[] parts = str.split("_");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Malformed ClusterRef: " + str);
        }

        try {
            int pid = Integer.parseInt(parts[0]);
            int localClusterId = Integer.parseInt(parts[1]);
            return new ClusterRef(pid, localClusterId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in ClusterRef: " + str, e);
        }
    }

    @Override
    public ClusterRef clone() {
        try {
            return (ClusterRef) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}

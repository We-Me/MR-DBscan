package com.htzproject.dbscan.ds;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@Data
public class MergeSet implements Writable {
    Set<ClusterRef> merge;

    public MergeSet() {
        merge = new HashSet<>();
    }

    public void add(ClusterRef c) {
        merge.add(c);
    }

    public void addAll(MergeSet other) {
        this.merge.addAll(other.merge);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MergeSet{");
        Iterator<ClusterRef> it = merge.iterator();
        while (it.hasNext()) {
            ClusterRef c = it.next();
            sb.append(c.toString());
            if (it.hasNext()) {
                sb.append(";");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    public static MergeSet parse(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("Invalid MergeSet string: " + str);
        }
        str = str.trim();
        if (!str.startsWith("MergeSet{") || !str.endsWith("}")) {
            throw new IllegalArgumentException("Malformed MergeSet: " + str);
        }

        String inner = str.substring("MergeSet{".length(), str.length() - 1);
        MergeSet ms = new MergeSet();
        if (!inner.isEmpty()) {
            String[] parts = inner.split(";");
            for (String p : parts) {
                ms.add(ClusterRef.parse(p));
            }
        }
        return ms;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(merge.size());
        for (ClusterRef c : merge) {
            c.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        merge.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ClusterRef c = new ClusterRef();
            c.readFields(in);
            merge.add(c);
        }
    }
}

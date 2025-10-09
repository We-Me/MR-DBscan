package com.htzproject.dbscan.ds;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class BoundWritable implements Writable {
    private final static double EPS = 1e-6;
    private double lower = Double.POSITIVE_INFINITY;
    private double upper = Double.NEGATIVE_INFINITY;

    public static BoundWritable of(double v) {
        return new BoundWritable(v, v);
    }

    public void merge(BoundWritable o) {
        if (o == null) return;
        if (o.lower < this.lower) this.lower = o.lower;
        if (o.upper > this.upper) this.upper = o.upper;
    }

    public void openUpper() {
        this.upper += BoundWritable.EPS;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(lower);
        out.writeDouble(upper);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lower = in.readDouble();
        upper = in.readDouble();
    }

    @Override
    public String toString() {
        return "[" + lower + "," + upper + ")";
    }

    public static BoundWritable parse(String s) {
        if (s == null || s.isEmpty()) return null;
        s = s.trim();
        if (!s.startsWith("[") || !s.endsWith(")")) {
            throw new IllegalArgumentException("Invalid BoundWritable format: " + s);
        }
        String content = s.substring(1, s.length() - 1);
        String[] parts = content.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid BoundWritable format: " + s);
        }
        double lower = Double.parseDouble(parts[0].trim());
        double upper = Double.parseDouble(parts[1].trim());
        return new BoundWritable(lower, upper);
    }
}

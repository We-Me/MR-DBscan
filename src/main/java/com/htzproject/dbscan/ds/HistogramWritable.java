package com.htzproject.dbscan.ds;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

@Getter
@Setter
@NoArgsConstructor
public class HistogramWritable implements Writable, Cloneable {
    private double lower;
    private double upper;
    private int numBuckets;
    private long count;
    private long[] histogram;

    public HistogramWritable(BoundWritable bound, int numBuckets) {
        this.lower = bound.getLower();
        this.upper = bound.getUpper();
        this.numBuckets = numBuckets;
        this.count = 0;
        this.histogram = new long[numBuckets];
    }

    public HistogramWritable(double lower, double upper, int numBuckets) {
        this.lower = lower;
        this.upper = upper;
        this.numBuckets = numBuckets;
        this.count = 0;
        this.histogram = new long[numBuckets];
    }

    public void addValue(double value) {
        if (value < lower) value = lower;
        if (value > upper) value = upper;
        int bucket = (int) ((value - lower) / (upper - lower) * numBuckets);
        if (bucket == histogram.length) bucket--;
        count++;
        histogram[bucket]++;
    }

    public void merge(HistogramWritable other) {
        if (this.numBuckets != other.numBuckets || this.lower != other.lower || this.upper != other.upper) {
            throw new IllegalArgumentException("Histogram mismatch");
        }
        this.count += other.count;
        for (int i = 0; i < histogram.length; i++) {
            this.histogram[i] += other.histogram[i];
        }
    }

    @Override
    public HistogramWritable clone() {
        try {
            HistogramWritable copy = (HistogramWritable) super.clone();
            copy.histogram = (this.histogram == null) ? null : Arrays.copyOf(this.histogram, this.histogram.length);
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(lower);
        out.writeDouble(upper);
        out.writeLong(count);
        out.writeInt(numBuckets);
        for (long c : histogram) {
            out.writeLong(c);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lower = in.readDouble();
        upper = in.readDouble();
        count = in.readLong();
        numBuckets = in.readInt();
        histogram = new long[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            histogram[i] = in.readLong();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Histogram{");
        sb.append("lower=").append(lower)
                .append(";upper=").append(upper)
                .append(";count=").append(count)
                .append(";numBuckets=").append(numBuckets)
                .append(";histogram=[");
        for (int i = 0; i < histogram.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(histogram[i]);
        }
        sb.append("]}");
        return sb.toString();
    }

    public static HistogramWritable parse(String str) {
        if (str == null || !str.startsWith("Histogram{")) {
            throw new IllegalArgumentException("Invalid input: " + str);
        }

        str = str.replace("\\=", "="); // 防止 Hadoop 转义
        str = str.substring(str.indexOf('{') + 1, str.lastIndexOf('}'));

        String[] parts = str.split(";");
        if (parts.length < 5) {
            throw new IllegalArgumentException("Invalid format: " + str);
        }

        double lower = Double.parseDouble(parts[0].split("=")[1]);
        double upper = Double.parseDouble(parts[1].split("=")[1]);
        long count = Long.parseLong(parts[2].split("=")[1]);
        int numBuckets = Integer.parseInt(parts[3].split("=")[1]);

        String histStr = parts[4];
        if (!histStr.startsWith("histogram=[")) {
            throw new IllegalArgumentException("Invalid histogram: " + str);
        }
        histStr = histStr.substring("histogram=[".length(), histStr.length() - 1);
        String[] hParts = histStr.split(",");
        long[] histogram = new long[hParts.length];
        for (int i = 0; i < hParts.length; i++) {
            histogram[i] = Long.parseLong(hParts[i]);
        }

        HistogramWritable hw = new HistogramWritable(lower, upper, numBuckets);
        hw.setCount(count);
        hw.setHistogram(histogram);
        return hw;
    }
}

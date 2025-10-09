package com.htzproject.dbscan.partitioning.bound;

import com.htzproject.dbscan.ds.BoundWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BoundReducer extends Reducer<IntWritable, BoundWritable, Text, Text> {
    private final Map<Integer, BoundWritable> bounds = new HashMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<BoundWritable> values, Context ctx)
            throws IOException, InterruptedException {
        BoundWritable bound = bounds.computeIfAbsent(key.get(), k -> new BoundWritable());
        for (BoundWritable v : values) bound.merge(v);
        bound.openUpper();
        ctx.write(new Text(key.toString()), new Text(bound.toString()));
    }
}

package com.htzproject.dbscan.partitioning.bound;

import com.htzproject.dbscan.ds.BoundWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BoundCombiner extends Reducer<IntWritable, BoundWritable, IntWritable, BoundWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<BoundWritable> values, Context ctx)
            throws IOException, InterruptedException {
        BoundWritable acc = new BoundWritable();
        for (BoundWritable v : values) acc.merge(v);
        ctx.write(key, acc);
    }
}

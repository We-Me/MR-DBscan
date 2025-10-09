package com.htzproject.dbscan.partitioning.bound;

import com.htzproject.dbscan.ds.BoundWritable;
import com.htzproject.dbscan.ds.PointWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BoundMapper extends Mapper<Object, PointWritable, IntWritable, BoundWritable> {

    @Override
    protected void map(Object key, PointWritable value, Context ctx) throws IOException, InterruptedException {
        double[] vec = value.getVec();
        for (int dim = 0; dim < vec.length; dim++) {
            double v = vec[dim];
            ctx.write(new IntWritable(dim), BoundWritable.of(v));
        }
    }
}

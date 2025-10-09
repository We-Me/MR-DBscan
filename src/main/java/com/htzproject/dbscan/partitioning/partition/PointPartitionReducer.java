package com.htzproject.dbscan.partitioning.partition;

import com.htzproject.dbscan.ds.PointWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PointPartitionReducer extends Reducer<IntWritable, PointWritable, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
            throws IOException, InterruptedException {

        String pidStr = key.toString();
        for (PointWritable p : values) {
            context.write(new Text(pidStr), new Text(p.toString()));
        }
    }
}

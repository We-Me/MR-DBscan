package com.htzproject.dbscan.partitioning.histogram;

import com.htzproject.dbscan.ds.HistogramWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class HistogramCombiner extends Reducer<IntWritable, HistogramWritable, IntWritable, HistogramWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<HistogramWritable> values, Context context)
            throws IOException, InterruptedException {
        Iterator<HistogramWritable> it = values.iterator();
        if (!it.hasNext()) return;

        HistogramWritable merged = it.next().clone();
        while (it.hasNext()) {
            merged.merge(it.next().clone());
        }
        context.write(key, merged);
    }
}

package com.htzproject.dbscan.partitioning.histogram;

import com.htzproject.dbscan.ds.HistogramWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HistogramReducer extends Reducer<IntWritable, HistogramWritable, Text, Text> {
    private final Map<Integer, HistogramWritable> histograms = new HashMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<HistogramWritable> values, Context ctx)
            throws IOException, InterruptedException {
        Iterator<HistogramWritable> it = values.iterator();
        if (!it.hasNext()) return;

        HistogramWritable firstCopy = it.next().clone();
        HistogramWritable acc = histograms.computeIfAbsent(key.get(), k -> firstCopy);
        while (it.hasNext()) {
            acc.merge(it.next().clone());
        }
        ctx.write(new Text(key.toString()), new Text(acc.toString()));
    }
}

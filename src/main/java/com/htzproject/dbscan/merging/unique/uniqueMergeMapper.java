package com.htzproject.dbscan.merging.unique;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class uniqueMergeMapper extends Mapper<LongWritable, Text, ClusterRef, MergeSet> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split("\t");
        if (parts.length < 2) return;

        ClusterRef clusterRef = ClusterRef.parse(parts[0].trim());
        MergeSet mergeSet = MergeSet.parse(parts[1].trim());

        context.write(clusterRef, mergeSet);
    }
}

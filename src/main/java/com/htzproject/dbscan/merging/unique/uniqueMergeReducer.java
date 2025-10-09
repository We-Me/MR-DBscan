package com.htzproject.dbscan.merging.unique;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class uniqueMergeReducer extends Reducer<ClusterRef, MergeSet, Text, Text> {

    @Override
    protected void reduce(ClusterRef key, Iterable<MergeSet> values, Context context)
            throws IOException, InterruptedException {
        MergeSet mergeSet = new MergeSet();
        for (MergeSet ms : values) {
            mergeSet.addAll(ms);
        }
        context.write(new Text(key.toString()), new Text(mergeSet.toString()));
    }
}

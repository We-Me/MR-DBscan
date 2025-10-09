package com.htzproject.dbscan.partitioning.histogram;

import com.htzproject.dbscan.ds.BoundWritable;
import com.htzproject.dbscan.ds.HistogramWritable;
import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class HistogramMapper extends Mapper<Object, PointWritable, IntWritable, HistogramWritable> {
    private final Map<Integer, BoundWritable> bounds = new HashMap<>();
    private int numBuckets;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        String boundsPathStr = conf.get(Params.BOUNDS_OUTPUT_PATH);
        if (boundsPathStr == null) {
            throw new IOException("Stage2 Missing " + Params.BOUNDS_OUTPUT_PATH);
        }
        Path boundsPath = new Path(boundsPathStr);
        FileSystem fs = boundsPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(boundsPath);
        if (status.isDirectory()) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(boundsPath, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-r-")) {
                    loadBoundsMap(fs, f.getPath());
                }
            }
        } else {
            loadBoundsMap(fs, boundsPath);
        }

        String bucketStr = conf.get(Params.BUCKETS);
        if (bucketStr == null) {
            throw new IOException("Stage2 Missing " + Params.BUCKETS);
        }
        numBuckets = Integer.parseInt(bucketStr);
    }

    @Override
    protected void map(Object key, PointWritable value, Context context) throws IOException, InterruptedException {
        double[] vec = value.getVec();
        for (int dim = 0; dim < vec.length; dim++) {
            HistogramWritable histogram = new HistogramWritable(bounds.get(dim), numBuckets);
            histogram.addValue(vec[dim]);
            context.write(new IntWritable(dim), histogram);
        }
    }

    private void loadBoundsMap(FileSystem fs, Path file) throws IOException {
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) continue;
                int dim = Integer.parseInt(parts[0].trim());
                BoundWritable bound = BoundWritable.parse(parts[1].trim());
                bounds.put(dim, bound);
            }
        }
    }
}

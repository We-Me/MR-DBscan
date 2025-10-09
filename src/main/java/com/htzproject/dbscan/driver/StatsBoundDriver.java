package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.BoundWritable;
import com.htzproject.dbscan.io.CsvInputFormat;
import com.htzproject.dbscan.partitioning.bound.BoundCombiner;
import com.htzproject.dbscan.partitioning.bound.BoundMapper;
import com.htzproject.dbscan.partitioning.bound.BoundReducer;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class StatsBoundDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.BOUNDS_OUTPUT_PATH), conf);

        Job job = Job.getInstance(conf, "dbscan-job1-bound-stats");
        job.setJarByClass(StatsBoundDriver.class);

        job.setInputFormatClass(CsvInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(conf.get(Params.CSV_INPUT_PATH)));

        job.setMapperClass(BoundMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BoundWritable.class);

        job.setCombinerClass(BoundCombiner.class);
        job.setReducerClass(BoundReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(conf.get(Params.BOUNDS_OUTPUT_PATH)));

        return job.waitForCompletion(true);
    }

    private static void checkAndNormalizePaths(Configuration conf) {
        String csvInput = requireNonEmpty(conf, Params.CSV_INPUT_PATH);
        String outPath = requireNonEmpty(conf, Params.BOUNDS_OUTPUT_PATH);
        conf.set(Params.CSV_INPUT_PATH, toHdfsUri(csvInput));
        conf.set(Params.BOUNDS_OUTPUT_PATH, toHdfsUri(outPath));
    }

    private static void cleanOutput(String outputPathStr, Configuration conf) throws Exception {
        Path outPath = new Path(outputPathStr);
        try (FileSystem fs = FileSystem.get(URI.create(outputPathStr), conf)) {
            if (fs.exists(outPath)) fs.delete(outPath, true);
        }
    }

    private static String requireNonEmpty(Configuration conf, String key) {
        String v = conf.get(key);
        if (v == null || v.trim().isEmpty())
            throw new IllegalArgumentException("Missing required config: " + key);
        return v.trim();
    }

    private static String toHdfsUri(String p) {
        if (p.startsWith("hdfs://")) return p;
        return new Path(p).toUri().toString();
    }
}

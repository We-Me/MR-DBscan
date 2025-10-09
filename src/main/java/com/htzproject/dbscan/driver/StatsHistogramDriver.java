package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.HistogramWritable;
import com.htzproject.dbscan.io.CsvInputFormat;
import com.htzproject.dbscan.partitioning.histogram.HistogramCombiner;
import com.htzproject.dbscan.partitioning.histogram.HistogramMapper;
import com.htzproject.dbscan.partitioning.histogram.HistogramReducer;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class StatsHistogramDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.HISTOGRAMS_OUTPUT_PATH), conf);

        Job job = Job.getInstance(conf, "dbscan-job2-histogram-stats");
        job.setJarByClass(StatsHistogramDriver.class);

        MultipleInputs.addInputPath(job,
                new Path(conf.get(Params.CSV_INPUT_PATH)),
                CsvInputFormat.class,
                HistogramMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(HistogramWritable.class);

        job.setCombinerClass(HistogramCombiner.class);
        job.setReducerClass(HistogramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(conf.get(Params.HISTOGRAMS_OUTPUT_PATH)));

        return job.waitForCompletion(true);
    }

    private static void checkAndNormalizePaths(Configuration conf) {
        String csvInput = requireNonEmpty(conf, Params.CSV_INPUT_PATH);
        String boundsPath = requireNonEmpty(conf, Params.BOUNDS_OUTPUT_PATH);
        String outPath = requireNonEmpty(conf, Params.HISTOGRAMS_OUTPUT_PATH);
        conf.set(Params.CSV_INPUT_PATH, toHdfsUri(csvInput));
        conf.set(Params.BOUNDS_OUTPUT_PATH, toHdfsUri(boundsPath));
        conf.set(Params.HISTOGRAMS_OUTPUT_PATH, toHdfsUri(outPath));
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

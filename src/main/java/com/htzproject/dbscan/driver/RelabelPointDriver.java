package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.merging.relabel.RelabelMapper;
import com.htzproject.dbscan.merging.relabel.RelabelReducer;
import com.htzproject.dbscan.util.Params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class RelabelPointDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.MR_CLUSTER_OUTPUT_PATH), conf);

        Job job = Job.getInstance(conf, "dbscan-job9-relabel-data");
        job.setJarByClass(RelabelPointDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(conf.get(Params.CLUSTER_OUTPUT_PATH)));

        job.setMapperClass(RelabelMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        job.setReducerClass(RelabelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(Params.MR_CLUSTER_OUTPUT_PATH)));

        return job.waitForCompletion(true);
    }

    /** 校验并标准化路径参数 */
    private static void checkAndNormalizePaths(Configuration conf) {
        String stage5 = requireNonEmpty(conf, Params.CLUSTER_OUTPUT_PATH);
        String stage8 = requireNonEmpty(conf, Params.GLOBAL_MERGE_OUTPUT_PATH);
        String stage9 = requireNonEmpty(conf, Params.MR_CLUSTER_OUTPUT_PATH);
        conf.set(Params.CLUSTER_OUTPUT_PATH, toHdfsUri(stage5));
        conf.set(Params.GLOBAL_MERGE_OUTPUT_PATH, toHdfsUri(stage8));
        conf.set(Params.MR_CLUSTER_OUTPUT_PATH, toHdfsUri(stage9));
    }

    /** 删除旧输出目录 */
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

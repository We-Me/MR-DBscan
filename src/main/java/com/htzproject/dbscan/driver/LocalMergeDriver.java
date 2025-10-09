package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.merging.local.LocalMergeMapper;
import com.htzproject.dbscan.merging.local.LocalMergeReducer;
import com.htzproject.dbscan.util.Params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.net.URI;

public class LocalMergeDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.LOCAL_MERGE_OUTPUT_PATH), conf);

        Job job = Job.getInstance(conf, "dbscan-job6-local-merge");
        job.setJarByClass(LocalMergeDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(conf.get(Params.CLUSTER_OUTPUT_PATH)));

        job.setMapperClass(LocalMergeMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        job.setReducerClass(LocalMergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(Params.LOCAL_MERGE_OUTPUT_PATH)));

        return job.waitForCompletion(true);
    }

    /** 校验路径参数并标准化为 HDFS URI */
    private static void checkAndNormalizePaths(Configuration conf) {
        String stage5 = requireNonEmpty(conf, Params.CLUSTER_OUTPUT_PATH);
        String output = requireNonEmpty(conf, Params.LOCAL_MERGE_OUTPUT_PATH);
        conf.set(Params.CLUSTER_OUTPUT_PATH, toHdfsUri(stage5));
        conf.set(Params.LOCAL_MERGE_OUTPUT_PATH, toHdfsUri(output));
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

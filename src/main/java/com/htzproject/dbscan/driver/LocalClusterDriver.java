package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.local_clustering.LocalClusterMapper;
import com.htzproject.dbscan.local_clustering.LocalClusterReducer;
import com.htzproject.dbscan.util.Params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class LocalClusterDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.CLUSTER_OUTPUT_PATH), conf);

        Job job = Job.getInstance(conf, "dbscan-job5-local-cluster");
        job.setJarByClass(LocalClusterDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(conf.get(Params.PARTITIONNODE_OUTPUT_PATH)));

        job.setMapperClass(LocalClusterMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        job.setReducerClass(LocalClusterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(conf.get(Params.CLUSTER_OUTPUT_PATH)));

        return job.waitForCompletion(true);
    }

    /** 校验路径参数并标准化为 HDFS URI */
    private static void checkAndNormalizePaths(Configuration conf) {
        String input = requireNonEmpty(conf, Params.PARTITIONNODE_OUTPUT_PATH);
        String output = requireNonEmpty(conf, Params.CLUSTER_OUTPUT_PATH);
        conf.set(Params.PARTITIONNODE_OUTPUT_PATH, toHdfsUri(input));
        conf.set(Params.CLUSTER_OUTPUT_PATH, toHdfsUri(output));
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

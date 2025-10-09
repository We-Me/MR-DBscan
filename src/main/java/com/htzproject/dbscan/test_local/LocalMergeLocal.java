package com.htzproject.dbscan.test_local;

import com.htzproject.dbscan.merging.local.LocalMergeMapper;
import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.merging.local.LocalMergeReducer;
import com.htzproject.dbscan.util.JobConfs;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;
import java.nio.file.Paths;

public class LocalMergeLocal {
    public static void main(String[] args) throws Exception {
        String jobConfPath = null;
        for (String a : args) {
            if (a.startsWith("--jobConf=")) {
                jobConfPath = a.substring("--jobConf=".length());
            }
        }

        Configuration conf = new Configuration(false);
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt("mapreduce.local.map.tasks.maximum",
                Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

        JobConfs.addJobConf(conf, jobConfPath);

        // 参数校验
        String stage5OutStr = requireNonEmpty(conf, Params.CLUSTER_OUTPUT_PATH);
        String outDirStr = requireNonEmpty(conf, Params.LOCAL_MERGE_OUTPUT_PATH);

        // URI 统一化
        String stage5Uri = toFileUri(stage5OutStr);
        String outUri = toFileUri(outDirStr);
        conf.set(Params.CLUSTER_OUTPUT_PATH, stage5Uri);
        conf.set(Params.LOCAL_MERGE_OUTPUT_PATH, outUri);

        Path outPath = new Path(outUri);
        try (FileSystem localFs = FileSystem.get(URI.create("file:///"), conf)) {
            if (localFs.exists(outPath)) {
                localFs.delete(outPath, true);
            }
        }

        // Job 定义
        Job job = Job.getInstance(conf, "dbscan-job6-local-merge");
        job.setJarByClass(LocalMergeLocal.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(stage5Uri));

        job.setMapperClass(LocalMergeMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        job.setReducerClass(LocalMergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }

    private static String requireNonEmpty(Configuration conf, String key) {
        String v = conf.get(key);
        if (v == null || v.trim().isEmpty()) {
            System.err.println("ERROR: missing required config: " + key);
            System.exit(2);
        }
        return v.trim();
    }

    private static String toFileUri(String p) {
        java.nio.file.Path abs = Paths.get(p).toAbsolutePath().normalize();
        return abs.toUri().toString();
    }
}

package com.htzproject.dbscan.test_local;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import com.htzproject.dbscan.merging.unique.uniqueMergeMapper;
import com.htzproject.dbscan.merging.unique.uniqueMergeReducer;
import com.htzproject.dbscan.util.JobConfs;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.Text;

import java.net.URI;
import java.nio.file.Paths;

public class UniqueMergeLocal {

    public static void main(String[] args) throws Exception {
        String jobConfPath = null;
        for (String a : args) {
            if (a.startsWith("--jobConf=")) {
                jobConfPath = a.substring("--jobConf=".length());
            }
        }

        // 本地执行环境配置
        Configuration conf = new Configuration(false);
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.setInt("mapreduce.local.map.tasks.maximum",
                Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

        JobConfs.addJobConf(conf, jobConfPath);

        // 参数读取与校验
        String stage6OutStr = requireNonEmpty(conf, Params.LOCAL_MERGE_OUTPUT_PATH);
        String outDirStr = requireNonEmpty(conf, Params.UNIQUE_MERGE_OUTPUT_PATH);

        // URI 规范化
        String stage6Uri = toFileUri(stage6OutStr);
        String outUri = toFileUri(outDirStr);
        conf.set(Params.LOCAL_MERGE_OUTPUT_PATH, stage6Uri);
        conf.set(Params.UNIQUE_MERGE_OUTPUT_PATH, outUri);

        Path outPath = new Path(outUri);
        try (FileSystem localFs = FileSystem.get(URI.create("file:///"), conf)) {
            if (localFs.exists(outPath)) {
                localFs.delete(outPath, true);
            }
        }

        // Job 定义
        Job job = Job.getInstance(conf, "dbscan-stage7-global-merge-local");
        job.setJarByClass(UniqueMergeLocal.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(stage6Uri));
        job.setMapperClass(uniqueMergeMapper.class);
        job.setMapOutputKeyClass(ClusterRef.class);
        job.setMapOutputValueClass(MergeSet.class);

        job.setReducerClass(uniqueMergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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


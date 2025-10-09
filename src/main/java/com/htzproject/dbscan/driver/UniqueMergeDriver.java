package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import com.htzproject.dbscan.merging.unique.uniqueMergeMapper;
import com.htzproject.dbscan.merging.unique.uniqueMergeReducer;
import com.htzproject.dbscan.util.Params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class UniqueMergeDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.UNIQUE_MERGE_OUTPUT_PATH), conf);

        Job job = Job.getInstance(conf, "dbscan-job7-unique-merge");
        job.setJarByClass(UniqueMergeDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(conf.get(Params.LOCAL_MERGE_OUTPUT_PATH)));

        job.setMapperClass(uniqueMergeMapper.class);
        job.setMapOutputKeyClass(ClusterRef.class);
        job.setMapOutputValueClass(MergeSet.class);

        job.setReducerClass(uniqueMergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(conf.get(Params.UNIQUE_MERGE_OUTPUT_PATH)));

        return job.waitForCompletion(true);
    }

    /** 校验路径参数并标准化为 HDFS URI */
    private static void checkAndNormalizePaths(Configuration conf) {
        String stage6 = requireNonEmpty(conf, Params.LOCAL_MERGE_OUTPUT_PATH);
        String stage7 = requireNonEmpty(conf, Params.UNIQUE_MERGE_OUTPUT_PATH);
        conf.set(Params.LOCAL_MERGE_OUTPUT_PATH, toHdfsUri(stage6));
        conf.set(Params.UNIQUE_MERGE_OUTPUT_PATH, toHdfsUri(stage7));
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

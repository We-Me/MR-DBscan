package com.htzproject.dbscan.test_local;

import com.htzproject.dbscan.io.CsvInputFormat;
import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.partitioning.partition.PointPartitionMapper;
import com.htzproject.dbscan.partitioning.partition.PointPartitionReducer;
import com.htzproject.dbscan.util.JobConfs;
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
import java.nio.file.Paths;

public class PointPartitionLocal {
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
        String csvInputPathStr = requireNonEmpty(conf, Params.CSV_INPUT_PATH);
        String outDirStr = requireNonEmpty(conf, Params.PARTITIONNODE_OUTPUT_PATH);
        String planFileStr = requireNonEmpty(conf, Params.PARTITIONS_OUTPUT_PATH);

        // 统一 URI 格式
        String csvUri = toFileUri(csvInputPathStr);
        String outUri = toFileUri(outDirStr);
        String planUri = toFileUri(planFileStr);

        conf.set(Params.CSV_INPUT_PATH, csvUri);
        conf.set(Params.PARTITIONNODE_OUTPUT_PATH, outUri);
        conf.set(Params.PARTITIONS_OUTPUT_PATH, planUri);

        Path outPath = new Path(outUri);
        try (FileSystem localFs = FileSystem.get(URI.create("file:///"), conf)) {
            if (localFs.exists(outPath)) {
                localFs.delete(outPath, true);
            }
        }

        // 任务定义
        Job job = Job.getInstance(conf, "dbscan-job4-partition[local]");
        job.setJarByClass(PointPartitionLocal.class);

        job.setInputFormatClass(CsvInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(csvUri));

        job.setMapperClass(PointPartitionMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        // 使用 Reducer 输出 <Text,Text>
        job.setReducerClass(PointPartitionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outPath);

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

package com.htzproject.dbscan.test_local;

import com.htzproject.dbscan.io.CsvInputFormat;
import com.htzproject.dbscan.ds.HistogramWritable;
import com.htzproject.dbscan.partitioning.histogram.HistogramCombiner;
import com.htzproject.dbscan.partitioning.histogram.HistogramMapper;
import com.htzproject.dbscan.partitioning.histogram.HistogramReducer;
import com.htzproject.dbscan.util.JobConfs;
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
import java.nio.file.Paths;

public class StatsHistogramLocal {
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
        conf.setInt("mapreduce.local.reduce.tasks.maximum", 2);

        JobConfs.addJobConf(conf, jobConfPath);

        String csvInputPathStr = requireNonEmpty(conf, Params.CSV_INPUT_PATH);
        String outDirStr = requireNonEmpty(conf, Params.HISTOGRAMS_OUTPUT_PATH);
        String csvinputUri = toFileUri(csvInputPathStr);
        String outUri = toFileUri(outDirStr);
        conf.set(Params.CSV_INPUT_PATH, csvinputUri);
        conf.set(Params.HISTOGRAMS_OUTPUT_PATH, outUri);

        Path outPath = new Path(outUri);
        try (FileSystem localFs = FileSystem.get(URI.create("file:///"), conf)) {
            if (localFs.exists(outPath)) {
                localFs.delete(outPath, true);
            }
        }

        // 运行
        Job job = Job.getInstance(conf, "dbscan-job2-histogram[local]");
        job.setJarByClass(StatsHistogramLocal.class);

        MultipleInputs.addInputPath(job, new Path(csvinputUri), CsvInputFormat.class, HistogramMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(HistogramWritable.class);

        job.setCombinerClass(HistogramCombiner.class);
        job.setReducerClass(HistogramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outPath);

        job.setNumReduceTasks(1); // 保证输出一个 props 文件

        boolean ok = job.waitForCompletion(true);
        System.exit(ok ? 0 : 1);
    }

    private static String requireNonEmpty(Configuration conf, String key) {
        String v = conf.get(key);
        if (v == null) {
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

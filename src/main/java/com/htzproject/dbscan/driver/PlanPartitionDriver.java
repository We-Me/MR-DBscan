package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.HistogramWritable;
import com.htzproject.dbscan.ds.Partition;
import com.htzproject.dbscan.partitioning.plan.PartitionTreeBuilder;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PlanPartitionDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.PARTITIONS_OUTPUT_PATH), conf);

        long maxPartitionSize = Long.parseLong(conf.get(Params.MAX_PARTITION_SIZE));

        Path histogramsPath = new Path(conf.get(Params.HISTOGRAMS_OUTPUT_PATH));
        FileSystem fs = histogramsPath.getFileSystem(conf);

        Map<Integer, HistogramWritable> hists = loadHistograms(fs, histogramsPath);

        List<Partition> parts = getPartitionWritables(hists, maxPartitionSize);
        Map<Integer, Partition> partitionMap = new LinkedHashMap<>();
        for (int i = 0; i < parts.size(); i++) {
            partitionMap.put(i, parts.get(i));
        }

        writeOutput(partitionMap, conf.get(Params.PARTITIONS_OUTPUT_PATH), conf);
        return true;
    }

    private static void checkAndNormalizePaths(Configuration conf) {
        requireNonEmpty(conf, Params.MAX_PARTITION_SIZE);
        String histogramsStr = requireNonEmpty(conf, Params.HISTOGRAMS_OUTPUT_PATH);
        String outStr = requireNonEmpty(conf, Params.PARTITIONS_OUTPUT_PATH);
        String histogramsUri = toHdfsUri(histogramsStr);
        String outUri = toHdfsUri(outStr);
        conf.set(Params.HISTOGRAMS_OUTPUT_PATH, histogramsUri);
        conf.set(Params.PARTITIONS_OUTPUT_PATH, outUri);
    }

    private static void cleanOutput(String outputPathStr, Configuration conf) throws Exception {
        Path outPath = new Path(outputPathStr);
        try (FileSystem fs = FileSystem.get(URI.create(outputPathStr), conf)) {
            if (fs.exists(outPath)) fs.delete(outPath, true);
        }
    }

    /** 读取所有直方图输出 */
    private static Map<Integer, HistogramWritable> loadHistograms(FileSystem fs, Path histogramsPath) throws IOException {
        Map<Integer, HistogramWritable> hists = new HashMap<>();
        FileStatus status = fs.getFileStatus(histogramsPath);
        if (status.isDirectory()) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(histogramsPath, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-r-")) {
                    loadHistogramFile(fs, f.getPath(), hists);
                }
            }
        } else {
            loadHistogramFile(fs, histogramsPath, hists);
        }
        return hists;
    }

    private static void loadHistogramFile(FileSystem fs, Path file, Map<Integer, HistogramWritable> hists) throws IOException {
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) continue;
                int dim = Integer.parseInt(parts[0].trim());
                HistogramWritable hist = HistogramWritable.parse(parts[1].trim());
                hists.put(dim, hist);
            }
        }
    }

    private static List<Partition> getPartitionWritables(Map<Integer, HistogramWritable> hists, long maxPartitionSize) {
        long totalCount = -1;
        for (HistogramWritable hist : hists.values()) {
            if (totalCount == -1) totalCount = hist.getCount();
            else if (totalCount != hist.getCount())
                throw new IllegalStateException("Histogram counts are inconsistent");
        }

        PartitionTreeBuilder builder = new PartitionTreeBuilder(hists);
        return builder.plan(totalCount, maxPartitionSize);
    }

    private static void writeOutput(Map<Integer, Partition> partitionMap, String outUri, Configuration conf) throws IOException {
        Path outDir = new Path(outUri);
        try (FileSystem fs = FileSystem.get(URI.create(outUri), conf)) {
            if (fs.exists(outDir)) fs.delete(outDir, true);
            try (FSDataOutputStream out = fs.create(new Path(outDir, "part-r-00000"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
                for (Map.Entry<Integer, Partition> e : partitionMap.entrySet()) {
                    writer.write(e.getKey() + "\t" + e.getValue());
                    writer.newLine();
                }
            }
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

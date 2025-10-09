package com.htzproject.dbscan.test_local;

import com.htzproject.dbscan.ds.HistogramWritable;
import com.htzproject.dbscan.ds.Partition;
import com.htzproject.dbscan.partitioning.plan.PartitionTreeBuilder;
import com.htzproject.dbscan.util.JobConfs;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlanPartitionLocal {
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
        JobConfs.addJobConf(conf, jobConfPath);

        long maxPartitionSize = Long.parseLong(requireNonEmpty(conf, Params.MAX_PARTITION_SIZE));
        String histogramsStr = requireNonEmpty(conf, Params.HISTOGRAMS_OUTPUT_PATH);
        String histogramsUri = toFileUri(histogramsStr);
        conf.set(Params.HISTOGRAMS_OUTPUT_PATH, histogramsUri);
        String outStr = requireNonEmpty(conf, Params.PARTITIONS_OUTPUT_PATH);
        String outUri = toFileUri(outStr);
        conf.set(Params.PARTITIONS_OUTPUT_PATH, outUri);

        Path histogramsPath = new Path(histogramsUri);
        FileSystem fs = histogramsPath.getFileSystem(conf);
        Map<Integer, HistogramWritable> hists = new HashMap<>();
        FileStatus status = fs.getFileStatus(histogramsPath);
        if (status.isDirectory()) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(histogramsPath, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-r-")) {
                    loadhistogramsMap(fs, f.getPath(), hists);
                }
            }
        } else {
            loadhistogramsMap(fs, histogramsPath, hists);
        }

        List<Partition> parts = getPartitionWritables(hists, maxPartitionSize);
        Map<Integer, Partition> partitionHashMap = new HashMap<>();
        for (int i = 0; i < parts.size(); i++) {
            partitionHashMap.put(i, parts.get(i));
        }

        try (FileSystem fs2 = FileSystem.get(URI.create(outUri), conf)) {
            try (FSDataOutputStream out = fs2.create(new Path(outUri, "part-r-00000"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

                for (Map.Entry<Integer, Partition> e : partitionHashMap.entrySet()) {
                    Text key = new Text(e.getKey().toString());
                    Text val = new Text(String.valueOf(e.getValue()));
                    writer.write(key + "\t" + val);
                    writer.newLine();
                }
            }
        }
    }

    private static void loadhistogramsMap(FileSystem fs, Path file, Map<Integer, HistogramWritable> hists) throws IOException {
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
            if (totalCount == -1) {
                totalCount = hist.getCount();
            } else if (totalCount != hist.getCount()) {
                throw new IllegalStateException("Histogram counts are inconsistent");
            }
        }

        PartitionTreeBuilder builder = new PartitionTreeBuilder(hists);
        return builder.plan(totalCount, maxPartitionSize);
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

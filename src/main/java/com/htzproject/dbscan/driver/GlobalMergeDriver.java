package com.htzproject.dbscan.driver;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import com.htzproject.dbscan.merging.global.ClusterMerger;
import com.htzproject.dbscan.util.Params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.*;

public class GlobalMergeDriver {

    public static boolean run(Configuration conf) throws Exception {
        checkAndNormalizePaths(conf);
        cleanOutput(conf.get(Params.GLOBAL_MERGE_OUTPUT_PATH), conf);

        Path uniquePath = new Path(conf.get(Params.UNIQUE_MERGE_OUTPUT_PATH));
        FileSystem fs = uniquePath.getFileSystem(conf);

        Map<ClusterRef, MergeSet> raw = loadMerge(fs, uniquePath);

        // 执行融合
        ClusterMerger merger = new ClusterMerger();
        merger.addEdges(raw);

        // 写出结果
        writeOutput(merger.getResult(), conf.get(Params.GLOBAL_MERGE_OUTPUT_PATH), conf);
        return true;
    }

    /** 读取 Stage7 输出（Unique Merge 结果） */
    private static Map<ClusterRef, MergeSet> loadMerge(FileSystem fs, Path uniqueMergePath) throws IOException {
        Map<ClusterRef, MergeSet> mergeMap = new HashMap<>();
        FileStatus status = fs.getFileStatus(uniqueMergePath);
        if (status.isDirectory()) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(uniqueMergePath, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-r-")) {
                    loadMergeFile(fs, f.getPath(), mergeMap);
                }
            }
        } else {
            loadMergeFile(fs, uniqueMergePath, mergeMap);
        }
        return mergeMap;
    }

    private static void loadMergeFile(FileSystem fs, Path file, Map<ClusterRef, MergeSet> mergeMap) throws IOException {
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) continue;
                ClusterRef ref = ClusterRef.parse(parts[0].trim());
                MergeSet set = MergeSet.parse(parts[1].trim());
                mergeMap.put(ref, set);
            }
        }
    }

    private static void writeOutput(Map<ClusterRef, Integer> result, String outUri, Configuration conf) throws IOException {
        Path outDir = new Path(outUri);
        try (FileSystem fs = FileSystem.get(URI.create(outUri), conf)) {
            if (fs.exists(outDir)) fs.delete(outDir, true);
            try (FSDataOutputStream out = fs.create(new Path(outDir, "part-r-00000"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
                for (Map.Entry<ClusterRef, Integer> e : result.entrySet()) {
                    writer.write(e.getKey() + "\t" + e.getValue());
                    writer.newLine();
                }
            }
        }
    }

    private static void checkAndNormalizePaths(Configuration conf) {
        String stage7 = requireNonEmpty(conf, Params.UNIQUE_MERGE_OUTPUT_PATH);
        String stage8 = requireNonEmpty(conf, Params.GLOBAL_MERGE_OUTPUT_PATH);
        conf.set(Params.UNIQUE_MERGE_OUTPUT_PATH, toHdfsUri(stage7));
        conf.set(Params.GLOBAL_MERGE_OUTPUT_PATH, toHdfsUri(stage8));
    }

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

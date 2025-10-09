package com.htzproject.dbscan.test_local;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import com.htzproject.dbscan.merging.global.ClusterMerger;
import com.htzproject.dbscan.util.JobConfs;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

public class globalMergeLocal {

    public static void main(String[] args) throws Exception {
        String jobConfPath = null;
        for (String a : args) {
            if (a.startsWith("--jobConf=")) {
                jobConfPath = a.substring("--jobConf=".length());
            }
        }

        // 本地执行环境配置
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");

        JobConfs.addJobConf(conf, jobConfPath);

        // 参数读取与校验
        String stage7OutStr = requireNonEmpty(conf, Params.UNIQUE_MERGE_OUTPUT_PATH);
        String outDirStr = requireNonEmpty(conf, Params.GLOBAL_MERGE_OUTPUT_PATH);

        // URI 规范化
        String stage7Uri = toFileUri(stage7OutStr);
        String outUri = toFileUri(outDirStr);
        conf.set(Params.UNIQUE_MERGE_OUTPUT_PATH, stage7Uri);
        conf.set(Params.GLOBAL_MERGE_OUTPUT_PATH, outUri);

        Path outPath = new Path(outUri);
        try (FileSystem localFs = FileSystem.get(URI.create("file:///"), conf)) {
            if (localFs.exists(outPath)) {
                localFs.delete(outPath, true);
            }
        }

        Path uniquePath = new Path(conf.get(Params.UNIQUE_MERGE_OUTPUT_PATH));
        FileSystem fs = uniquePath.getFileSystem(conf);
        Map<ClusterRef, MergeSet> raw = loadMerge(fs, uniquePath);

        // 融合处理
        ClusterMerger merger = new ClusterMerger();
        merger.addEdges(raw);

        // 输出结果
        try (FileSystem fs2 = FileSystem.get(URI.create(outUri), conf)) {
            try (FSDataOutputStream out = fs2.create(new Path(outUri, "part-r-00000"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

                Map<ClusterRef, Integer> result = merger.getResult();
                for (Map.Entry<ClusterRef, Integer> e : result.entrySet()) {
                    Text key = new Text(e.getKey().toString());
                    Text val = new Text(String.valueOf(e.getValue()));
                    writer.write(key + "\t" + val);
                    writer.newLine();
                }
            }
        }
    }

    private static Map<ClusterRef, MergeSet> loadMerge(FileSystem fs, Path uniqueMergePath) throws IOException {
        Map<ClusterRef, MergeSet> mergeMap = new HashMap<>();
        FileStatus status = fs.getFileStatus(uniqueMergePath);
        if (status.isDirectory()) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(uniqueMergePath, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-r-")) {
                    loadmergeFile(fs, f.getPath(), mergeMap);
                }
            }
        } else {
            loadmergeFile(fs, uniqueMergePath, mergeMap);
        }
        return mergeMap;
    }

    private static void loadmergeFile(FileSystem fs, Path file, Map<ClusterRef, MergeSet> hists) throws IOException {
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) continue;
                ClusterRef clusterRef = ClusterRef.parse(parts[0].trim());
                MergeSet mergeSet = MergeSet.parse(parts[1].trim());
                hists.put(clusterRef, mergeSet);
            }
        }
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

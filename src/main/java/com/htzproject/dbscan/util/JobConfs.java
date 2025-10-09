package com.htzproject.dbscan.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public final class JobConfs {
    private JobConfs() {}

    public static void addJobConf(Configuration conf, String jobConfPath) throws IOException {
        boolean loaded = false;

        if (jobConfPath != null && !jobConfPath.isEmpty()) {
            if (jobConfPath.startsWith("hdfs://") || jobConfPath.startsWith("file:/")) {
                // 支持 HDFS 或 file://
                Path p = new Path(jobConfPath);
                FileSystem fs = FileSystem.get(URI.create(jobConfPath), conf);
                if (fs.exists(p)) {
                    conf.addResource(p);
                    loaded = true;
                } else {
                    throw new IOException("HDFS path not found: " + jobConfPath);
                }
            } else {
                // 普通本地路径
                File f = new File(jobConfPath);
                if (f.exists() && f.isFile()) {
                    conf.addResource(new Path(f.getAbsolutePath()));
                    loaded = true;
                }
            }
        }

        if (!loaded) {
            // fallback 到 classpath
            conf.addResource("job.xml");
        }
    }
}


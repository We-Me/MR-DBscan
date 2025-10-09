package com.htzproject.dbscan.partitioning.partition;

import com.htzproject.dbscan.ds.*;
import com.htzproject.dbscan.ds.tag.PartitionTag;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class PointPartitionMapper extends Mapper<Object, PointWritable, IntWritable, PointWritable> {
    private final Map<Integer, Partition> partitions = new HashMap<>();
    private double epsilon;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        String planPathStr = conf.get(Params.PARTITIONS_OUTPUT_PATH);
        if (planPathStr == null) {
            throw new IOException("Stage4 Missing " + Params.PARTITIONS_OUTPUT_PATH);
        }
        Path planPath = new Path(planPathStr);
        FileSystem fs = planPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(planPath);
        if (status.isDirectory()) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(planPath, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-r-")) {
                    loadPartitionsMap(fs, f.getPath());
                }
            }
        } else {
            loadPartitionsMap(fs, planPath);
        }

        String epsilonStr = conf.get(Params.EPSILON);
        if (epsilonStr == null) throw new IOException("Stage4 Missing" + Params.EPSILON);
        epsilon = Double.parseDouble(epsilonStr);
    }

    @Override
    protected void map(Object key, PointWritable point, Context context)
            throws IOException, InterruptedException {

        for (Map.Entry<Integer, Partition> entry : partitions.entrySet()) {
            int pid = entry.getKey();
            Partition partition = entry.getValue();

            PartitionTag tag = partition.getPointRoleInPartition(point, epsilon);
            if (tag != null) {
                PointWritable outPoint = new PointWritable(point, pid, tag);
                context.write(new IntWritable(pid), outPoint);
            }
        }
    }

    private void loadPartitionsMap(FileSystem fs, Path file) throws IOException {
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) continue;
                int pid = Integer.parseInt(parts[0].trim());
                Partition partition = Partition.parse(parts[1].trim());
                partitions.put(pid, partition);
            }
        }
    }
}

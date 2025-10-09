package com.htzproject.dbscan.local_clustering;

import com.htzproject.dbscan.ds.PointWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocalClusterMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // 按第一个 tab 分割成 pid 和 pointStr
        String[] parts = line.split("\t", 2);
        if (parts.length < 2) return;

        int pid = Integer.parseInt(parts[0].trim());
        String pointStr = parts[1].trim();

        PointWritable point = PointWritable.parse(pointStr);
        context.write(new IntWritable(pid), point);
    }
}


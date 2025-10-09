package com.htzproject.dbscan.merging.local;

import com.htzproject.dbscan.ds.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LocalMergeMapper extends Mapper<LongWritable, Text, LongWritable, PointWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // 按第一个 tab 分割成 pid 和 pointStr
        String[] parts = line.split("\t", 2);
        if (parts.length < 2) return;

        String pointStr = parts[1].trim();
        PointWritable point = PointWritable.parse(pointStr);

        context.write(new LongWritable(point.getId()), point);
    }
}

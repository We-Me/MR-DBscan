package com.htzproject.dbscan.local_clustering;

import com.htzproject.dbscan.core.SequentialDBSCAN;
import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LocalClusterReducer extends Reducer<IntWritable, PointWritable, Text, Text> {
    private int minPts;
    private double epsilon;

    @Override
    protected void setup(Reducer.Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        String epsilonStr = conf.get(Params.EPSILON);
        if (epsilonStr == null) throw new IOException("Stage4 Missing" + Params.EPSILON);
        epsilon = Double.parseDouble(epsilonStr);

        String minPtsStr = conf.get(Params.MINPTS);
        if (minPtsStr == null) throw new IOException("Stage4 Missing" + Params.MINPTS);
        minPts = Integer.parseInt(minPtsStr);
    }

    @Override
    protected void reduce(IntWritable pid, Iterable<PointWritable> points, Context ctx)
            throws IOException, InterruptedException {

        List<PointWritable> localPoints = new ArrayList<>();
        for (PointWritable p : points) {
            localPoints.add(p.clone());
        }

        SequentialDBSCAN sequentialDbscan = new SequentialDBSCAN(localPoints);
        sequentialDbscan.run(minPts, epsilon);

        String pidStr = pid.toString();
        for (PointWritable p : localPoints) {
            ctx.write(new Text(pidStr), new Text(p.toString()));
        }
    }
}

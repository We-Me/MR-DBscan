package com.htzproject.dbscan.merging.relabel;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class RelabelReducer extends Reducer<LongWritable, PointWritable, NullWritable, Text> {
    private final Map<ClusterRef, Integer> globalClusterMap = new HashMap<>();

    @Override
    protected void setup(Context ctx) throws IOException {
        Configuration conf = ctx.getConfiguration();
        String filePath = conf.get(Params.GLOBAL_MERGE_OUTPUT_PATH);
        if (filePath == null) {
            throw new IOException("Stage9 Missing " + Params.GLOBAL_MERGE_OUTPUT_PATH);
        }

        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(path);
        if (status.isDirectory()) {
            // 遍历目录下所有 part-* 文件
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);
            while (files.hasNext()) {
                LocatedFileStatus f = files.next();
                if (f.isFile() && f.getPath().getName().startsWith("part-")) {
                    loadGlobalMap(fs, f.getPath());
                }
            }
        } else {
            // 单个文件
            loadGlobalMap(fs, path);
        }
    }

    @Override
    protected void reduce(LongWritable pointId, Iterable<PointWritable> points, Context context)
            throws IOException, InterruptedException {
        List<PointWritable> listPoint = new ArrayList<>();
        for (PointWritable p : points) {
            listPoint.add(p.clone());
        }
        // 审查: 所有点的 globalClusterId 必须一致

        double[] baseVec = Arrays.copyOf(listPoint.get(0).getVec(), listPoint.get(0).getVec().length);
        Integer globalId = -1;
        for (PointWritable p : listPoint) {
            if (p.getLocalClusterId() == -1) continue;

            // 检查向量一致性
            double[] v = p.getVec();
            if (v.length != baseVec.length) {
                throw new IOException("Inconsistent vector length for point " + pointId);
            }
            for (int i = 0; i < v.length; i++) {
                if (Double.compare(v[i], baseVec[i]) != 0) {
                    throw new IOException("Inconsistent vector value for point " + pointId +
                            " at index " + i + ": " + v[i] + " vs " + baseVec[i]);
                }
            }

            ClusterRef ref = new ClusterRef(p.getPartitionID(), p.getLocalClusterId());
            Integer gid = globalClusterMap.get(ref);
            if (gid == null) {
                throw new IOException("ClusterRef " + ref + " has no global id mapping");
            }
            if (globalId == -1) {
                globalId = gid;
            } else if (!globalId.equals(gid)) {
                throw new IOException("Inconsistent globalClusterId for point " + pointId +
                        ": got " + globalId + " and " + gid);
            }
        }

        context.write(NullWritable.get(), new Text(pointId + "\t" + globalId + "\t" + Arrays.toString(baseVec)));
    }

    private void loadGlobalMap(FileSystem fs, Path file) throws IOException {
        try (FSDataInputStream in = fs.open(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] kv = line.split("\t");
                if (kv.length != 2) continue;
                ClusterRef c = ClusterRef.parse(kv[0]);
                int gid = Integer.parseInt(kv[1]);
                globalClusterMap.put(c, gid);
            }
        }
    }
}

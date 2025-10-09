package com.htzproject.dbscan.merging.local;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;
import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.ds.tag.ClusterTag;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LocalMergeReducer extends Reducer<LongWritable, PointWritable, Text, Text> {

    @Override
    protected void reduce(LongWritable pointId, Iterable<PointWritable> points, Context context)
            throws IOException, InterruptedException {
        List<PointWritable> listPoint = new ArrayList<>();
        for (PointWritable p : points) {
            listPoint.add(p.clone());
        }

        int n = listPoint.size();
        if (n <= 1) {
            PointWritable p = listPoint.get(0);
            ClusterRef one = new ClusterRef(p.getPartitionID(), p.getLocalClusterId());
            MergeSet mergeSet = new MergeSet();
            mergeSet.add(one);
            context.write(new Text(one.toString()), new Text(mergeSet.toString()));
        }

        Map<ClusterRef, MergeSet> mergeMap = new HashMap<>();
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                PointWritable pi = listPoint.get(i);
                PointWritable pj = listPoint.get(j);

                if (pi.getCTag() == ClusterTag.NOISE || pj.getCTag() == ClusterTag.NOISE)
                    continue;

                if (pi.getCTag() == ClusterTag.CORE || pj.getCTag() == ClusterTag.CORE) {
                    ClusterRef a = new ClusterRef(pi.getPartitionID(), pi.getLocalClusterId());
                    ClusterRef b = new ClusterRef(pj.getPartitionID(), pj.getLocalClusterId());
                    merge(mergeMap, a, b);
                }
            }
        }

        for (Map.Entry<ClusterRef, MergeSet> e : mergeMap.entrySet()) {
            context.write(new Text(e.getKey().toString()), new Text(e.getValue().toString()));
        }
    }

    // 合并两个簇所在集合（并查集式）
    private void merge(Map<ClusterRef, MergeSet> map, ClusterRef a, ClusterRef b) {
        MergeSet groupA = map.get(a);
        MergeSet groupB = map.get(b);

        if (groupA == null && groupB == null) {
            MergeSet newSet = new MergeSet();
            newSet.add(a);
            newSet.add(b);
            map.put(a, newSet);
            map.put(b, newSet);
        } else if (groupA != null && groupB == null) {
            groupA.add(b);
            map.put(b, groupA);
        } else if (groupA == null) {
            groupB.add(a);
            map.put(a, groupB);
        } else if (groupA != groupB) {
            groupA.addAll(groupB);
            for (ClusterRef x : groupB.getMerge()) {
                map.put(x, groupA);
            }
        }
    }
}

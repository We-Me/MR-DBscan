package com.htzproject.dbscan.merging.global;

import com.htzproject.dbscan.ds.ClusterRef;
import com.htzproject.dbscan.ds.MergeSet;

import java.util.HashMap;
import java.util.Map;

public final class ClusterMerger {
    private final Map<ClusterRef, MergeSet> map = new HashMap<>();

    public void merge(ClusterRef a, ClusterRef b) {
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

    public void addEdges(Map<ClusterRef, MergeSet> input) {
        for (Map.Entry<ClusterRef, MergeSet> e : input.entrySet()) {
            ClusterRef key = e.getKey();
            for (ClusterRef neighbor : e.getValue().getMerge()) {
                merge(key, neighbor);
            }
        }
    }

    /**
     * 返回 ClusterRef -> globalClusterId 的映射
     * localClusterId = -1 的保持 -1
     * 其他的按 MergeSet 分配全局 ID
     */
    public Map<ClusterRef, Integer> getResult() {
        Map<ClusterRef, Integer> result = new HashMap<>();

        // 已分配过 globalId 的 MergeSet
        Map<MergeSet, Integer> assigned = new HashMap<>();
        int nextGlobalId = 0;

        for (Map.Entry<ClusterRef, MergeSet> entry : map.entrySet()) {
            ClusterRef ref = entry.getKey();
            MergeSet set = entry.getValue();

            if (ref.getLocalClusterId() == -1) {
                result.put(ref, -1);
                continue;
            }

            Integer gid = assigned.get(set);
            if (gid == null) {
                gid = nextGlobalId++;
                assigned.put(set, gid);
            }
            result.put(ref, gid);
        }

        return result;
    }
}
package com.htzproject.dbscan.core;

import com.htzproject.dbscan.ds.tag.ClusterTag;
import com.htzproject.dbscan.ds.PointWritable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class SequentialDBSCAN {
    private int clusterId = 0;
    private final List<PointWritable> data;

    public SequentialDBSCAN(List<PointWritable> data) {
        this.data = data;
    }

    public void run(int minPts, double eps) {
        for (PointWritable p : data) {
            if (p.isVisited()) continue;
            p.setVisited(true);

            List<PointWritable> neighbors = getNeighbors(p, eps);
            if (neighbors.size() < minPts) {
                p.setCTag(ClusterTag.NOISE);
            } else {
                p.setLocalClusterId(clusterId);
                p.setCTag(ClusterTag.CORE);
                expandCluster(neighbors, minPts, eps);
                clusterId++;
            }
        }
    }

    private void expandCluster(List<PointWritable> neighbors,
                               int minPts,
                               double eps) {
        LinkedList<PointWritable> queue = new LinkedList<>(neighbors);

        while (!queue.isEmpty()) {
            PointWritable q = queue.poll();
            if (!q.isVisited()) {
                q.setVisited(true);
                q.setLocalClusterId(clusterId);
                List<PointWritable> qNeighbors = getNeighbors(q, eps);
                if (qNeighbors.size() >= minPts) {
                    q.setCTag(ClusterTag.CORE);
                    queue.addAll(qNeighbors);
                } else {
                    q.setCTag(ClusterTag.BORDER);
                }
            } else if (q.getCTag() == ClusterTag.NOISE) {
                q.setLocalClusterId(clusterId);
                q.setCTag(ClusterTag.BORDER);
            }
        }
    }

    private List<PointWritable> getNeighbors(PointWritable p, double eps) {
        List<PointWritable> neighbors = new ArrayList<>();
        for (PointWritable q : data) {
            if (p != q && distance(p, q) < eps) {
                neighbors.add(q);
            }
        }
        return neighbors;
    }

    private double distance(PointWritable p, PointWritable q) {
        double[] a = p.getVec();
        double[] b = q.getVec();
        if (a == null || b == null || a.length != b.length) {
            return Double.POSITIVE_INFINITY;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}

package com.htzproject.dbscan.partitioning.plan;

import com.htzproject.dbscan.ds.BoundWritable;
import com.htzproject.dbscan.ds.HistogramWritable;
import com.htzproject.dbscan.ds.Partition;

import java.util.*;

public final class PartitionTreeBuilder {
    private final Map<Integer, HistogramWritable> globalHists;

    public PartitionTreeBuilder(Map<Integer, HistogramWritable> hists) {
        this.globalHists = hists;
    }

    private static class BucketRange {
        int from, to;
        BucketRange(int from, int to) { this.from = from; this.to = to; }
    }

    private static class PartitionNode {
        Map<Integer, BucketRange> ranges;
        long count;
        int splitDim;
        int splitBucket;
        PartitionNode left;
        PartitionNode right;

        PartitionNode(Map<Integer, BucketRange> ranges,
                      long count,
                      int splitDim,
                      int splitBucket,
                      PartitionNode left,
                      PartitionNode right) {
            this.ranges = ranges;
            this.count = count;
            this.splitDim = splitDim;
            this.splitBucket = splitBucket;
            this.left = left;
            this.right = right;
        }

        boolean isLeaf() {
            return splitDim < 0;
        }
    }

    public List<Partition> plan(long totalCount, long maxPartitionSize) {
        Map<Integer, BucketRange> initRanges = new HashMap<>();
        for (Map.Entry<Integer, HistogramWritable> e : globalHists.entrySet()) {
            initRanges.put(e.getKey(), new BucketRange(0, e.getValue().getNumBuckets()));
        }
        PartitionNode root = buildTree(initRanges, totalCount, maxPartitionSize);

        List<Partition> leaves = new ArrayList<>();
        collectLeaves(root, leaves);
        return leaves;
    }

    private void collectLeaves(PartitionNode node, List<Partition> result) {
        if (node.isLeaf()) {
            Map<Integer, BoundWritable> bounds = new HashMap<>();
            for (Map.Entry<Integer, BucketRange> e : node.ranges.entrySet()) {
                HistogramWritable hist = globalHists.get(e.getKey());
                int numBuckets = hist.getNumBuckets();
                double min = hist.getLower();
                double max = hist.getUpper();

                int from = e.getValue().from;
                double lower = min + (max - min) * from / numBuckets;
                int to = e.getValue().to;
                double upper = min + (max - min) * to / numBuckets;

                bounds.put(e.getKey(), new BoundWritable(lower, upper));
            }
            result.add(new Partition(bounds));
            return;
        }
        if (node.left != null) collectLeaves(node.left, result);
        if (node.right != null) collectLeaves(node.right, result);
    }

    private PartitionNode buildTree(Map<Integer, BucketRange> ranges,
                                    long curCount,
                                    long maxPartitionSize) {
        if (curCount <= maxPartitionSize) {
            return new PartitionNode(ranges, curCount, -1, -1, null, null);
        }

        int splitDim = chooseSplitDim(ranges);
        if (splitDim == -1) {
            return new PartitionNode(ranges, curCount, -1, -1, null, null);
        }
        BucketRange range = ranges.get(splitDim);
        HistogramWritable hist = globalHists.get(splitDim);
        int splitBucket = chooseSplitBucket(hist, range, curCount);

        Map<Integer, BucketRange> leftRanges = new HashMap<>(ranges);
        leftRanges.put(splitDim, new BucketRange(range.from, splitBucket));
        Map<Integer, BucketRange> rightRanges = new HashMap<>(ranges);
        rightRanges.put(splitDim, new BucketRange(splitBucket, range.to));

        long leftCount = (long) (countRateInRange(hist, range.from, splitBucket) * curCount);
        long rightCount = curCount - leftCount;

        PartitionNode left = buildTree(leftRanges, leftCount, maxPartitionSize);
        PartitionNode right = buildTree(rightRanges, rightCount, maxPartitionSize);

        return new PartitionNode(ranges, curCount, splitDim, splitBucket, left, right);
    }

    private int chooseSplitDim(Map<Integer, BucketRange> ranges) {
        int bestDim = -1;
        double bestVar = -1;
        for (Map.Entry<Integer, BucketRange> e : ranges.entrySet()) {
            if (e.getValue().from + 1 == e.getValue().to) continue;

            HistogramWritable hist = globalHists.get(e.getKey());
            long[] slice = Arrays.copyOfRange(hist.getHistogram(),
                    e.getValue().from, e.getValue().to);
            double var = variance(slice);
            if (var > bestVar) {
                bestVar = var;
                bestDim = e.getKey();
            }
        }
        return bestDim;
    }

    private double variance(long[] arr) {
        if (arr.length == 0) return 0;
        double mean = Arrays.stream(arr).average().orElse(0);
        double var = 0;
        for (long v : arr) var += (v - mean) * (v - mean);
        return var / arr.length;
    }

    private int chooseSplitBucket(HistogramWritable hist,
                                  BucketRange range,
                                  long totalCount) {
        long[] bins = hist.getHistogram();

        // 1. 找到区间内的最小桶（谷底）
        int minBucket = range.from;
        long minVal = Long.MAX_VALUE;
        for (int i = range.from; i < range.to; i++) {
            if (bins[i] < minVal) {
                minVal = bins[i];
                minBucket = i;
            }
        }

        // 2. 判断谷底是否在中间1/3范围内
        int oneThird = (range.to - range.from) / 3;
        int midStart = range.from + oneThird;
        int midEnd = range.to - oneThird;
        if (minBucket >= midStart && minBucket < midEnd) {
            return minBucket + 1;
        }

        // 3. fallback：中位桶
        long half = totalCount / 2;
        long prefix = 0;
        for (int i = range.from; i < range.to; i++) {
            prefix += bins[i];
            if (prefix >= half) return i;
        }
        return (range.from + range.to) / 2 + 1;
    }

    private double countRateInRange(HistogramWritable hist, int from, int to) {
        long numerator = Arrays.stream(hist.getHistogram(), from, to).sum();
        long denominator = Arrays.stream(hist.getHistogram(), 0, hist.getHistogram().length).sum();
        if (denominator == 0) return 0.0; // 避免除零
        return (double) numerator / denominator;
    }
}

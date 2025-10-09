package com.htzproject.dbscan.ds;

import com.htzproject.dbscan.ds.tag.PartitionTag;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Partition {
    private Map<Integer, BoundWritable> bounds;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Partition{");
        Iterator<Map.Entry<Integer, BoundWritable>> it = bounds.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, BoundWritable> e = it.next();
            sb.append(e.getKey()).append(":").append(e.getValue());
            if (it.hasNext()) {
                sb.append(";");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    public static Partition parse(String s) {
        if (s == null || !s.startsWith("Partition{") || !s.endsWith("}")) {
            throw new IllegalArgumentException("Invalid Partition string: " + s);
        }
        Map<Integer, BoundWritable> bounds = new HashMap<>();
        s = s.replace("\\:", ":");
        String inner = s.substring("Partition{".length(), s.length() - 1).trim();
        if (inner.isEmpty()) {
            return new Partition();
        }
        String[] entries = inner.split(";");
        for (String entry : entries) {
            String[] kv = entry.split(":", 2);
            if (kv.length != 2) continue;
            int key = Integer.parseInt(kv[0].trim());
            BoundWritable val = BoundWritable.parse(kv[1].trim());
            bounds.put(key, val);
        }
        return new Partition(bounds);
    }

    public PartitionTag getPointRoleInPartition(PointWritable point, double epsilon) {
        boolean hasOuter = false;
        boolean hasInnerMargin = false;

        for (Map.Entry<Integer, BoundWritable> entry : bounds.entrySet()) {
            int dim = entry.getKey();
            BoundWritable bound = entry.getValue();
            double v = point.getVec()[dim];

            double lower = bound.getLower();
            double high = bound.getUpper();

            // 不在扩展边界范围
            if (v < lower - epsilon || v >= high + epsilon) {
                return null;
            }

            // OUTER_MARGIN
            if ((lower - epsilon <= v && v < lower) || (high <= v && v < high + epsilon)) {
                hasOuter = true;
            }
            // INNER_MARGIN
            else if ((lower <= v && v < lower + epsilon) || (high - epsilon <= v && v < high)) {
                hasInnerMargin = true;
            }
            // 否则就是 INNER_REGION，不改变状态
        }

        if (hasOuter) {
            return PartitionTag.OUTER_MARGIN;
        } else if (hasInnerMargin) {
            return PartitionTag.INNER_MARGIN;
        } else {
            return PartitionTag.INNER_REGION;
        }
    }
}

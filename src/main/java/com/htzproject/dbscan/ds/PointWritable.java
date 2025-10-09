package com.htzproject.dbscan.ds;

import com.htzproject.dbscan.ds.tag.ClusterTag;
import com.htzproject.dbscan.ds.tag.PartitionTag;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PointWritable implements WritableComparable<PointWritable>, Cloneable {
    private long id = -1L;
    private double[] vec = null;
    private PartitionTag pTag = PartitionTag.UNASSIGNED;
    private ClusterTag cTag = ClusterTag.UNASSIGNED;
    private int partitionID = -1;
    private int localClusterId = -1;
    private boolean visited = false;
    private int globalClusterId = -1;

    public PointWritable(PointWritable other, int pid, PartitionTag tag) {
        this.id = other.id;
        this.vec = (other.vec == null) ? null : Arrays.copyOf(other.vec, other.vec.length);
        this.pTag = tag;
        this.cTag = other.cTag;
        this.partitionID = pid;
        this.localClusterId = other.localClusterId;
        this.visited = other.visited;
        this.globalClusterId = other.globalClusterId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeByte(pTag.ordinal());
        out.writeByte(cTag.ordinal());
        out.writeInt(partitionID);
        out.writeInt(localClusterId);
        out.writeBoolean(visited);
        out.writeInt(globalClusterId);

        int dim = (vec == null) ? 0 : vec.length;
        out.writeInt(dim);
        for (int i = 0; i < dim; i++) {
            out.writeDouble(vec[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        pTag = PartitionTag.fromOrdinal(in.readByte());
        cTag = ClusterTag.fromOrdinal(in.readByte());
        partitionID = in.readInt();
        localClusterId = in.readInt();
        visited = in.readBoolean();
        globalClusterId = in.readInt();

        int dim = in.readInt();
        vec = new double[dim];
        for (int i = 0; i < dim; i++) {
            vec[i] = in.readDouble();
        }
    }

    @Override
    public PointWritable clone() {
        try {
            PointWritable copy = (PointWritable) super.clone();
            copy.vec = (this.vec == null) ? null : Arrays.copyOf(this.vec, this.vec.length);
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public int compareTo(@NotNull PointWritable o) {
        // 1. globalClusterId
        int cmp = Integer.compare(this.globalClusterId, o.globalClusterId);
        if (cmp != 0) return cmp;

        // 2. partitionID
        cmp = Integer.compare(this.partitionID, o.partitionID);
        if (cmp != 0) return cmp;

        // 3. localClusterId
        cmp = Integer.compare(this.localClusterId, o.localClusterId);
        if (cmp != 0) return cmp;

        // 4. id
        return Long.compare(this.id, o.id);
    }

    @Override
    public String toString() {
        return "PointWritable{" +
                "id=" + id +
                ";pTag=" + pTag +
                ";cTag=" + cTag +
                ";partitionID=" + partitionID +
                ";localClusterId=" + localClusterId +
                ";visited=" + visited +
                ";globalClusterId=" + globalClusterId +
                ";vec=" + Arrays.toString(vec) +
                '}';
    }

    public static PointWritable parse(String s) {
        if (s == null || !s.startsWith("PointWritable{") || !s.endsWith("}")) {
            throw new IllegalArgumentException("Invalid PointWritable string: " + s);
        }

        String inner = s.substring("PointWritable{".length(), s.length() - 1);
        String[] parts = inner.split(";");

        PointWritable p = new PointWritable();
        for (String part : parts) {
            String[] kv = part.split("=", 2);
            if (kv.length != 2) continue;
            String key = kv[0].trim();
            String val = kv[1].trim();

            switch (key) {
                case "id":
                    p.setId(Integer.parseInt(val));
                    break;
                case "pTag":
                    p.setPTag(PartitionTag.valueOf(val));
                    break;
                case "cTag":
                    p.setCTag(ClusterTag.valueOf(val));
                    break;
                case "partitionID":
                    p.setPartitionID(Integer.parseInt(val));
                    break;
                case "localClusterId":
                    p.setLocalClusterId(Integer.parseInt(val));
                    break;
                case "visited":
                    p.setVisited(Boolean.parseBoolean(val));
                    break;
                case "globalClusterId":
                    p.setGlobalClusterId(Integer.parseInt(val));
                    break;
                case "vec":
                    val = val.substring(1, val.length() - 1).trim();
                    if (val.isEmpty()) {
                        p.setVec(new double[0]);
                    } else {
                        String[] nums = val.split(",");
                        double[] arr = new double[nums.length];
                        for (int i = 0; i < nums.length; i++) {
                            arr[i] = Double.parseDouble(nums[i].trim());
                        }
                        p.setVec(arr);
                    }
                    break;
                default:
                    // ignore
            }
        }
        return p;
    }
}

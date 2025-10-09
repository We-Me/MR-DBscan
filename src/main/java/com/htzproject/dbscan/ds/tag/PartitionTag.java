package com.htzproject.dbscan.ds.tag;

public enum PartitionTag {
    UNASSIGNED,
    INNER_REGION,
    INNER_MARGIN,
    OUTER_MARGIN;

    /** 从数值编码还原枚举，越界回退到 UNASSIGNED */
    public static PartitionTag fromOrdinal(int code) {
        if (code == 1) return INNER_REGION;
        if (code == 2) return INNER_MARGIN;
        if (code == 3) return OUTER_MARGIN;
        return UNASSIGNED;
    }

    /** 转为字节编码 */
    public byte toByte() {
        return (byte) this.ordinal();
    }
}

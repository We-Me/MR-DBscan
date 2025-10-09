package com.htzproject.dbscan.ds.tag;

public enum ClusterTag {
    UNASSIGNED,
    CORE,
    BORDER,
    NOISE;

    /** 从数值编码还原枚举，越界回退到 UNASSIGNED */
    public static ClusterTag fromOrdinal(int code) {
        if (code == 1) return CORE;
        if (code == 2) return BORDER;
        if (code == 3) return NOISE;
        return UNASSIGNED;
    }

    /** 转为字节编码 */
    public byte toByte() {
        return (byte) this.ordinal();
    }
}

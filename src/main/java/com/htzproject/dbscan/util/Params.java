package com.htzproject.dbscan.util;

public final class Params {
    private Params() {}

    public static final String EPSILON = "dbscan.epsilon";
    public static final String MINPTS = "dbscan.minpts";
    public static final String BUCKETS = "dbscan.stage2.buckets";
    public static final String MAX_PARTITION_SIZE = "dbscan.stage3.size";

    public static final String CSV_INPUT_PATH = "dbscan.csv.input";
    public static final String CSV_DELIM = "dbscan.csv.delim";
    public static final String CSV_HAS_HEADER = "dbscan.csv.header";
    public static final String CSV_HAS_ID = "dbscan.csv.id";

    public static final String BOUNDS_OUTPUT_PATH = "dbscan.stage1.output";
    public static final String HISTOGRAMS_OUTPUT_PATH = "dbscan.stage2.output";
    public static final String PARTITIONS_OUTPUT_PATH = "dbscan.stage3.output";
    public static final String PARTITIONNODE_OUTPUT_PATH = "dbscan.stage4.output";
    public static final String CLUSTER_OUTPUT_PATH = "dbscan.stage5.output";
    public static final String LOCAL_MERGE_OUTPUT_PATH = "dbscan.stage6.output";
    public static final String UNIQUE_MERGE_OUTPUT_PATH = "dbscan.stage7.output";
    public static final String GLOBAL_MERGE_OUTPUT_PATH = "dbscan.stage8.output";
    public static final String MR_CLUSTER_OUTPUT_PATH = "dbscan.stage9.output";
}

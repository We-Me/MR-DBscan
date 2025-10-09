package com.htzproject.dbscan;

import com.htzproject.dbscan.driver.*;
import com.htzproject.dbscan.util.JobConfs;
import org.apache.hadoop.conf.Configuration;

public final class Main {
    public static void main(String[] args) throws Exception {
        String jobConfPath = null;
        String stageArg = "all";

        for (String a : args) {
            if (a.startsWith("--jobConf=")) {
                jobConfPath = a.substring("--jobConf=".length());
            } else if (a.startsWith("--stage=")) {
                stageArg = a.substring("--stage=".length()).trim().toLowerCase();
            }
        }

        if (jobConfPath == null || jobConfPath.isEmpty()) {
            System.err.println("Usage: Main --jobConf=<path> [--stage=<1-9|all>]");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        JobConfs.addJobConf(conf, jobConfPath);

        switch (stageArg) {
            case "1":
                runStage1(conf);
                break;
            case "2":
                runStage2(conf);
                break;
            case "3":
                runStage3(conf);
                break;
            case "4":
                runStage4(conf);
                break;
            case "5":
                runStage5(conf);
                break;
            case "6":
                runStage6(conf);
                break;
            case "7":
                runStage7(conf);
                break;
            case "8":
                runStage8(conf);
                break;
            case "9":
                runStage9(conf);
                break;
            case "all":
                runAll(conf);
                break;
            default:
                System.err.println("Invalid --stage value: " + stageArg);
                System.exit(2);
        }
    }

    private static void runAll(Configuration conf) throws Exception {
        if (!StatsBoundDriver.run(conf)) System.exit(1);
        if (!StatsHistogramDriver.run(conf)) System.exit(2);
        if (!PlanPartitionDriver.run(conf)) System.exit(3);
        if (!PointPartitionDriver.run(conf)) System.exit(4);
        if (!LocalClusterDriver.run(conf)) System.exit(5);
        if (!LocalMergeDriver.run(conf)) System.exit(6);
        if (!UniqueMergeDriver.run(conf)) System.exit(7);
        if (!GlobalMergeDriver.run(conf)) System.exit(8);
        if (!RelabelPointDriver.run(conf)) System.exit(9);
    }

    private static void runStage1(Configuration conf) throws Exception {
        if (!StatsBoundDriver.run(conf)) System.exit(1);
    }

    private static void runStage2(Configuration conf) throws Exception {
        if (!StatsHistogramDriver.run(conf)) System.exit(2);
    }

    private static void runStage3(Configuration conf) throws Exception {
        if (!PlanPartitionDriver.run(conf)) System.exit(3);
    }

    private static void runStage4(Configuration conf) throws Exception {
        if (!PointPartitionDriver.run(conf)) System.exit(4);
    }

    private static void runStage5(Configuration conf) throws Exception {
        if (!LocalClusterDriver.run(conf)) System.exit(5);
    }

    private static void runStage6(Configuration conf) throws Exception {
        if (!LocalMergeDriver.run(conf)) System.exit(6);
    }

    private static void runStage7(Configuration conf) throws Exception {
        if (!UniqueMergeDriver.run(conf)) System.exit(7);
    }

    private static void runStage8(Configuration conf) throws Exception {
        if (!GlobalMergeDriver.run(conf)) System.exit(8);
    }

    private static void runStage9(Configuration conf) throws Exception {
        if (!RelabelPointDriver.run(conf)) System.exit(9);
    }
}

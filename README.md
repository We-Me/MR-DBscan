# MR-DBSCAN

## 项目简介

**MR-DBSCAN** 是一个使用 **MapReduce** 实现的 **DBSCAN 密度聚类算法**。在处理海量数据集时，传统单机 DBSCAN 算法往往受限于内存与计算能力。本项目通过 **Hadoop MapReduce** 框架将算法划分为多个阶段，实现并行的密度聚类，支持在 HDFS 上直接处理大型 CSV 数据集。

---

## 主要特性

- **分布式运行**：基于 Hadoop MapReduce 架构，支持集群并行计算。
- **参数化配置**：通过 XML（如 `job.xml`）定义算法参数与输入输出路径。
- **模块化阶段设计**：每个阶段均对应一个独立的 MapReduce Job，可单独运行或串行执行。
- **自定义 CSV 读取器**：支持带/不带 header、ID 列的 CSV 文件读取。
- **可扩展性强**：便于在现有 Hadoop 生态中集成。

---

## 环境要求

| 项目组件 | 版本 |
|-----------|------|
| JDK       | 11 或以上 |
| Hadoop    | 3.3.6 |
| Maven     | 3.6+ |
| Lombok    | 1.18.30 |
| SLF4J     | 1.7.36 |

---

## 目录结构概览

```
MR-DBscan/
├── src/main/java/com/htzproject/dbscan/        # 核心代码包
│   ├── driver/                                 # 每个 MapReduce 阶段的驱动类
│   ├── ds/                                     # 点与簇等数据结构
│   ├── io/                                     # 自定义输入格式，如 CsvInputFormat
│   ├── local_clustering/                       # 分区内聚类的 Mapper/Reducer 实现
│   ├── merging/                                # 聚类结果合并逻辑
│   └── util/                                   # 参数与配置读取工具
├── src/main/resources/job.xml                  # 默认配置文件
└── pom.xml                                     # Maven 构建配置

```

### 运行方式

运行 MapReduce 任务前，需准备输入数据并配置参数：

1. 准备数据：将待聚类的数据集保存为 CSV 格式，每行表示一个样本向量。可根据实际情况选择是否在第一列包含样本 ID，并通过配置决定是否忽略表头。

2. 配置参数：项目通过 Hadoop Configuration 读取参数。可以使用默认的job.xml，也可以新建自定义配置文件，常用参数包括：

   - dbscan.epsilon：邻域半径ε，决定点之间的距离阈值
   - dbscan.minpts：最小邻居数量minPts，用于判断核心点
   - dbscan.stage2.buckets：第二阶段直方图分桶数量
   - dbscan.stage3.size：第三阶段每个分区的最大点数，用于均衡负载
   - dbscan.csv.delim：CSV 分隔符
   - dbscan.csv.header：布尔值，指示是否存在表头
   - dbscan.csv.id：布尔值，指示是否第一列为样本 ID
   - dbscan.stage{N}.output：各阶段输出目录，可根据需要修改

3. 提交作业：打包后可通过 Hadoop 命令或直接运行 Jar 启动任务。例如，在集群环境下可以使用:
    ```bash
   hadoop jar target/DBscan-1.0-SNAPSHOT-with-deps.jar \
    --jobConf=/path/to/your/job.xml \
    --stage=all
   ```
    其中 --stage 参数可指定运行的阶段：

   | 阶段编号 | 驱动类                    | 说明                  |
   | ---- | ---------------------- | ------------------- |
   | 1    | `StatsBoundDriver`     | 计算数据边界，生成全局范围       |
   | 2    | `StatsHistogramDriver` | 统计特征直方图，用于估计密度      |
   | 3    | `PlanPartitionDriver`  | 规划数据分区，决定划分方案       |
   | 4    | `PointPartitionDriver` | 根据规划将点写入对应分区        |
   | 5    | `LocalClusterDriver`   | 在各分区内执行局部 DBSCAN 聚类 |
   | 6    | `LocalMergeDriver`     | 合并分区内聚类结果           |
   | 7    | `UniqueMergeDriver`    | 处理跨分区唯一簇合并          |
   | 8    | `GlobalMergeDriver`    | 对所有分区执行全局合并         |
   | 9    | `RelabelPointDriver`   | 给最终聚类结果重新编号         |


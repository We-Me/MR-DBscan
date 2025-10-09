package com.htzproject.dbscan.io;

import com.htzproject.dbscan.ds.PointWritable;
import com.htzproject.dbscan.util.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvInputFormat extends FileInputFormat<LongWritable, PointWritable> {

    @Override
    public RecordReader<LongWritable, PointWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new CsvPointRecordReader();
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> files = super.listStatus(job);
        List<FileStatus> valid = new ArrayList<>();
        for (FileStatus f : files) {
            if (!f.isDirectory()
                    && f.getPath().getName().toLowerCase().endsWith(".csv")) {
                valid.add(f);
            }
        }
        return valid;
    }

    public static class CsvPointRecordReader extends RecordReader<LongWritable, PointWritable> {
        private final LineRecordReader lineReader = new LineRecordReader();
        private final LongWritable key = new LongWritable();
        private final PointWritable value = new PointWritable();

        private String delim;
        private boolean hasHeader;
        private boolean hasId;

        private long lineNum = -1;
        private int featFrom;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            lineReader.initialize(split, context);
            Configuration conf = context.getConfiguration();

            this.delim = conf.get(Params.CSV_DELIM);
            if (this.delim == null)
                throw new IOException("Missing required configuration: " + Params.CSV_DELIM);

            String headerStr = conf.get(Params.CSV_HAS_HEADER);
            if (headerStr == null)
                throw new IOException("Missing required configuration: " + Params.CSV_HAS_HEADER);
            this.hasHeader = Boolean.parseBoolean(headerStr);

            String idStr = conf.get(Params.CSV_HAS_ID);
            if (idStr == null)
                throw new IOException("Missing required configuration: " + Params.CSV_HAS_ID);
            this.hasId = Boolean.parseBoolean(idStr);

            this.featFrom = this.hasId ? 1 : 0;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            while (true) {
                if (!lineReader.nextKeyValue()) return false;

                key.set(lineReader.getCurrentKey().get());
                String line = lineReader.getCurrentValue().toString().trim();
                if (line.isEmpty()) continue;

                lineNum++;
                if (hasHeader && lineNum == 0) continue;

                String[] toks = line.split(delim, -1);
                if (toks.length <= featFrom) continue; // 无特征数据行跳过

                long id = hasId ? Long.parseLong(toks[0]) : lineNum;
                double[] vec = new double[toks.length - featFrom];
                for (int i = featFrom; i < toks.length; i++) {
                    vec[i - featFrom] = Double.parseDouble(toks[i]);
                }

                value.setId(id);
                value.setVec(vec);
                return true;
            }
        }

        @Override public LongWritable getCurrentKey() { return key; }
        @Override public PointWritable getCurrentValue() { return value; }
        @Override public float getProgress() throws IOException { return lineReader.getProgress(); }
        @Override public void close() throws IOException { lineReader.close(); }
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }
}

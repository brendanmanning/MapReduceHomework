package com.brendanmanning.HW3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class UniqueWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Text mapper_key = new Text();
    private final static IntWritable unique_vals = new IntWritable(1);

    @Override
    protected void map(
       LongWritable key,
       Text value,
       Mapper<LongWritable, Text, Text, IntWritable>.Context context
    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(),"\n");

        mapper_key.set("unique");
        unique_vals.set(itr.countTokens());

        context.write(mapper_key, unique_vals);

    }

}
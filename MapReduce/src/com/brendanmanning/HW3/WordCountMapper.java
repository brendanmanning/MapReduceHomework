package com.brendanmanning.HW3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class WordCountMapper extends Mapper <LongWritable, Text, Text, IntWritable>
{
    private final static Text word = new Text();
    private final static IntWritable count = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // Create a string tokenizer on the lowercase content using only the space
        StringTokenizer itr = new StringTokenizer(value.toString(), " ");

        // Loop through all the available tokens
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, count);
        }
    }

}
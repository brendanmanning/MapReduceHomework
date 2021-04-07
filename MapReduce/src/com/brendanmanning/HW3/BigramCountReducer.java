package com.brendanmanning.HW3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BigramCountReducer extends Reducer <Text, IntWritable, Text, IntWritable>
{
    private IntWritable count = new IntWritable();

    public void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        int cnt = 0;
        for (IntWritable val : values) {
            cnt += val.get();
        }

        count.set(cnt);
        context.write(key, count);

    }
}
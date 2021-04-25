package com.brendanmanning.HW3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UniqueTermCount {
    public static void main(String[] args) throws Exception {

        // Run a temporary word count
        WordWithLetterCount.main(new String[]{args[0], "/hdfs/temporary_wordcount_output"});

        // Create a new job using the output from the last task
        Configuration conf = new Configuration();

        String[] pathArgs = new GenericOptionsParser(
                conf, new String[]{ "/hdfs/temporary_wordcount_output/part-r-*", args[1] }
        ).getRemainingArgs();

        // Configure job
        Job job = Job.getInstance(conf, "UniqueTermCount");
        job.setJarByClass(UniqueTermCount.class);
        job.setMapperClass(UniqueWordMapper.class);
        job.setReducerClass(UniqueWordReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(pathArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));

        // Run and print the result & release an exit code
        job.waitForCompletion(true);

    }
}
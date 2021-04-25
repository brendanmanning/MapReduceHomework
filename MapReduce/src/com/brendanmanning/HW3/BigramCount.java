package com.brendanmanning.HW3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BigramCount
{
    public static void main(String[] args) throws Exception
    {

        // Extract arguments from the command line
        Configuration conf = new Configuration();
        String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Make sure we have the correct number of arguments
        if (pathArgs.length != 2) {
            System.err.println("BigramCount command takes arguments: <input-path> <output-path>");
            System.exit(1);
        }

        // Create a MapReduce job
        Job job = Job.getInstance(conf, "BigramCount");

        // Connect the classes we created
        job.setJarByClass(BigramCount.class);
        job.setMapperClass(BigramCountMapper.class);
        job.setCombinerClass(BigramCountReducer.class);
        job.setReducerClass(BigramCountReducer.class);
        //job.setSortComparatorClass(IntComparator.class);
        //job.setNumReduceTasks(5);

        // Use default input output classes
        // input: Text (Strings) - content from a book
        // output: IntWritable (int) - word counts
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add the input file and output folder
        FileInputFormat.addInputPath(job, new Path(pathArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));

        // Run and print the result & release an exit code
        boolean success = job.waitForCompletion(true);
        System.out.println(success ? "Run successful" : "Run unsuccessful");
        System.exit(success ? 1 : 0);

    }
}
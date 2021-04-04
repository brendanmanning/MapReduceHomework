package com.brendanmanning.HW3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class WordCount
{
    public static void main(String[] args) throws Exception
    {

        // Extract arguments from the command line
        Configuration conf = new Configuration();
        String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Make sure we have the correct number of arguments
        if (pathArgs.length != 2) {
            System.err.println("WordCount command takes arguments: <input-path> <output-path>");
            System.exit(1);
        }

        // Create a Hadoop job
        Job wcJob = Job.getInstance(conf, "WordCount");

        // Connect the classes we created
        wcJob.setJarByClass(WordCount.class);
        wcJob.setMapperClass(WordCountMapper.class);
        wcJob.setCombinerClass(WordCountReducer.class);
        wcJob.setReducerClass(WordCountReducer.class);
        wcJob.setNumReduceTasks(5);

        // Use default input output classes
        // input: Text (Strings) - content from a book
        // output: IntWritable (int) - word counts
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(IntWritable.class);

        // Add the input file and output folder
        FileInputFormat.addInputPath(wcJob, new Path(pathArgs[0]));
        FileOutputFormat.setOutputPath(wcJob, new Path(pathArgs[1]));

        // Run and print the result & release an exit code
        boolean success = wcJob.waitForCompletion(true);
        System.out.println(success ? "Run successful" : "Run unsuccessful");
        System.exit(success ? 1 : 0);

    }
}
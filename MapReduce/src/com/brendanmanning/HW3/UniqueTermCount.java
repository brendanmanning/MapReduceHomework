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

    /*
        //THE DRIVER CODE FOR MR CHAIN
        Configuration conf1=new Configuration();

        Job j1=Job.getInstance(conf1);
        j1.setJarByClass(WordCount.class);
        j1.setMapperClass(WordCountMapper.class);
        j1.setReducerClass(WordCountReducer.class);

//        j1.setMapOutputKeyClass(Text.class);
//        j1.setMapOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(IntWritable.class);
        j1.setOutputValueClass(Text.class);

        Path outputPath = new Path("/hdfs/temp-1");
        FileInputFormat.addInputPath(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1,outputPath);
        outputPath.getFileSystem(conf1).delete(outputPath);
        j1.waitForCompletion(true);

        Configuration conf2=new Configuration();
        Job j2=Job.getInstance(conf2);
        j2.setJarByClass(UniqueTermCount.class);
        j2.setMapperClass(UniqueWordMapper.class);
        j2.setNumReduceTasks(0);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);
        Path outputPath1=new Path(args[1]);
        FileInputFormat.addInputPath(j2, outputPath);
        FileOutputFormat.setOutputPath(j2, outputPath1);
        outputPath1.getFileSystem(conf2).delete(outputPath1, true);

        System.exit(j2.waitForCompletion(true)?0:1); */

        // Extract arguments from the command line
        Configuration conf = new Configuration();

        String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        pathArgs[0] = "/hdfs/wordwithlettercountoutput/part-r-*";
        pathArgs[1] = "/hdfs/uniquewordanalysisoutput";
        System.out.println("input: " + pathArgs[0]);
        System.out.println("output: " + pathArgs[1]);

        Job job = Job.getInstance(conf, "UniqueTermCount");
        job.setJarByClass(UniqueTermCount.class);
        job.setMapperClass(UniqueWordMapper.class);
        job.setReducerClass(UniqueWordReducer.class);
        job.setNumReduceTasks(1); // originally was 0

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(pathArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));

        // Run and print the result & release an exit code
        boolean success = job.waitForCompletion(true);

        return;

    }

    /*

        // Make sure we have the correct number of arguments
        if (pathArgs.length != 2) {
            System.err.println("UniqueTermCount command takes arguments: <input-path> <output-path>");
            System.exit(1);
        }

        // Create a Hadoop job
        Job job = Job.getInstance(conf, "UniqueTermCount");

        // Connect the classes we created
        job.setJarByClass(UniqueTermCount.class);
        job.setMapperClass(UniqueWordMapper.class);
        //job.setCombinerClass(WordWithLetterCountReducer.class);
        //job.setReducerClass(UniqueWordReducer.class);
        job.setNumReduceTasks(1);

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

     */
}
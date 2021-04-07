package com.brendanmanning.HW3;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class BigramCountMapper extends Mapper <LongWritable, Text, Text, IntWritable>
{
    private final static Text bigram = new Text();
    private final static IntWritable count = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // Create a string tokenizer on the lowercase content using only the space
        List<String> bigrams = new ArrayList<String>();

        System.out.println("Value: " + value.toString());

        StringTokenizer itr = new StringTokenizer(value.toString()," ");
        if(itr.countTokens() > 1) {
            System.out.println("\ttokens: " + itr.countTokens());
            String s1 = "";
            String s2 = "";
            while (itr.hasMoreTokens()) {
                if(s1.isEmpty()) {
                    s1 = itr.nextToken();
                }
                s2 = itr.nextToken();
                System.out.println("\t\tadding: " + s1 + " " + s2);
                bigrams.add(s1 + " " + s2);
            }
            System.out.println("Exiting loop");
        } else {
            System.out.println("Just one token");
        }

        // Loop through all the bigrams
        for(int i = 0; i < bigrams.size(); i++) {
            String bg = bigrams.get(i);
            System.out.println("Writing a bigram " + bg + " ...");
            bigram.set(bg);
            context.write(bigram, count);
        }

    }

    private List<String> getBigrams(String input) {
        String[] components = input.split(" ");
        ArrayList<String> bigrams = new ArrayList<String>();
        for(int i = 0; i < components.length - 2; i++) {
            bigrams.add(components[i] + " " + components[i+1]);
        }
        return bigrams;
    }

}

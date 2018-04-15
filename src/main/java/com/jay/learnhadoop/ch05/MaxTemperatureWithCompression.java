package com.jay.learnhadoop.ch05;

import com.jay.learnhadoop.ch02.MaxTemperatureMapper;
import com.jay.learnhadoop.ch02.MaxTemperatureReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxTemperatureWithCompression {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperatureWithCompression.class);
        job.setJobName("MaxTemperature");

        FileInputFormat.addInputPath(job, new Path("file:///home/jay/IdeaProjects/LearnHadoop/input/ncdc/sample.txt.gz"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

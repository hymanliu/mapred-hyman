package com.hyman.hadoop.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobMain {

	public static void main(String[] args) throws Exception {
		
		if(args.length<2){
			System.err.println("params:<in> <out>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "avg-job");
		
		job.setJarByClass(JobMain.class);
		
		job.setMapperClass(AvgMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(AvgReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[1]);
		if(fs.exists(path))
		{
			fs.delete(path,true);
			System.exit(1);
		}
		FileOutputFormat.setOutputPath(job, path);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}

package com.hyman.hadoop.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TemplateMapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";

	static class TemplateMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		}
		
	}
	
	static class TemplateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		
		}
		
	}
	
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, TemplateMapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(TemplateMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(TemplateMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setCombinerClass(null);
		//job.setSortComparatorClass(cls);
		//job.setGroupingComparatorClass(cls);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(TemplateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[1]);
		if(fs.exists(path))
		{
			fs.delete(path, true);
		}
		FileOutputFormat.setOutputPath(job, path);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static void main(String [] args) throws Exception{
		
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/template/input",
				HOST+"/user/ehp/mapred/template/output"
			};
		}
		
		System.exit(new TemplateMapReduce().run(args));
	}
}

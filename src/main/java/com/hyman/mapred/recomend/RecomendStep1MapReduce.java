package com.hyman.mapred.recomend;

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
import org.hsqldb.lib.StringUtil;

public class RecomendStep1MapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	
	//uid	pid	datetime
	
	//pid uid times
	static class RecomendMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		static IntWritable ONE = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if(StringUtil.isEmpty(line)) return;
			
			String[] arr = line.split("\t");
			context.write(new Text(arr[1]+"\t"+arr[0]), ONE);
			
		}
		
	}
	
	static class RecomendReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) 
				throws IOException, InterruptedException {
			int total = 0;
			for(IntWritable value :values){
				total+=value.get();
			}
			context.write(key, new IntWritable(total));
		}
		
	}
	
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, RecomendStep1MapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(RecomendStep1MapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(RecomendMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setCombinerClass(null);
		//job.setSortComparatorClass(cls);
		//job.setGroupingComparatorClass(cls);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(RecomendReducer.class);
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
				HOST+"/user/ehp/mapred/recomend/input",
				HOST+"/user/ehp/mapred/recomend/step1/output"
			};
		}
		
		System.exit(new RecomendStep1MapReduce().run(args));
	}
}

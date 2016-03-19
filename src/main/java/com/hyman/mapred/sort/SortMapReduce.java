package com.hyman.mapred.sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class SortMapReduce extends Configured implements Tool  {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	static class SortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(StringUtils.isNotBlank(value.toString())){
				int val = Integer.parseInt(value.toString().trim());
				context.write(new IntWritable(val), new IntWritable(val));
			}
		}
		
	}
	
	static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable>{


		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for(IntWritable val : values){
				context.write(val, NullWritable.get());
			}
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, SortMapReduce.class.getName());
		job.setJarByClass(SortMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
	
		Path path = new Path(args[1]);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		
		FileOutputFormat.setOutputPath(job,path);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/sort/input",
				HOST+"/user/ehp/mapred/sort/output"
			};
		}
		

		System.exit(new SortMapReduce().run(args));
	}


	

}

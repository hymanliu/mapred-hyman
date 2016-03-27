package com.hyman.mapred.index;

import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class IndexMapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";

	static class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{

		private String fileName = null;
		private static Text ONE = new Text("1");
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			FileSplit split = (FileSplit) context.getInputSplit();
			fileName = split.getPath().getName();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if(line.length()>0){
				StringTokenizer tokenizer = new StringTokenizer(line);
				while(tokenizer.hasMoreTokens()){
					String word = tokenizer.nextToken();
					context.write(new Text(word+"\t"+fileName), ONE);
				}
			}
			
		}
		
	}
	
	static class IndexCombiner extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException {
			String[] arr = key.toString().split("\t");
			int total = 0;
			for(Text val : values){
				total+=Integer.parseInt(val.toString());
			}
			context.write(new Text(arr[0]), new Text(arr[1]+":"+total));
		}
	}
	
	
	
	static class IndexReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text val : values){
				sb.append(val).append(";");
			}
			sb.deleteCharAt(sb.length()-1);
			context.write(key, new Text(sb.toString()));
		}
		
	}
	
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, IndexMapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(IndexMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setPartitionerClass(HashPartitioner.class);
		
		job.setCombinerClass(IndexCombiner.class);
		//job.setSortComparatorClass(cls);
		//job.setGroupingComparatorClass(cls);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
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
				HOST+"/user/ehp/mapred/index/input",
				HOST+"/user/ehp/mapred/index/output"
			};
		}
		
		System.exit(new IndexMapReduce().run(args));
	}
}

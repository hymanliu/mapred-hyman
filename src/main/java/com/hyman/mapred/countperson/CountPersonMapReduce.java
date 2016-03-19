package com.hyman.mapred.countperson;

import java.io.IOException;

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


public class CountPersonMapReduce extends Configured implements Tool {
	
	//Hyman|M|20
	
	//count num
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";

	static class CountPersonMapper extends Mapper<LongWritable, Text, NullWritable,Text>{
		
		int manNum = 0;
		int womanNum = 0;
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] arr = value.toString().split("\\|");
			String sex = arr[1];
			
			if("M".equals(sex)){
				manNum++;
			}else{
				womanNum++;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			context.write(NullWritable.get(),new Text(manNum+"\t"+womanNum));
		}
		
	}
	
	static class CountPersonReducer extends Reducer<NullWritable, Text, Text, IntWritable>{

		int NumMan = 0;
		int NumWoman = 0;
		
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			for(Text text : values){
				String[] arr = text.toString().split("\t");
				NumMan += Integer.parseInt(arr[0]);
				NumWoman += Integer.parseInt(arr[1]);
			}
			
			context.write(new Text("M"), new IntWritable(NumMan));
			context.write(new Text("F"), new IntWritable(NumWoman));
		}
		
	}
	
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, CountPersonMapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(CountPersonMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(CountPersonMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setCombinerClass(null);
		//job.setSortComparatorClass(cls);
		//job.setGroupingComparatorClass(cls);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(CountPersonReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[1]);
		if(fs.exists(path))
		{
			//output file path exists.
			fs.delete(path, true);
			//System.exit(1);
		}
		FileOutputFormat.setOutputPath(job, path);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static void main(String [] args) throws Exception{
		
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/count/input",
				HOST+"/user/ehp/mapred/count/output"
			};
		}
		
		System.exit(new CountPersonMapReduce().run(args));
		
	}
}

package com.hyman.mapred.recomend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class RecomendStep2MapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	
	//uid	pid	datetime
	//select uid ,contact(sum(pid)) group by uid
	//uid1	pid1:4|pid2:3|pid3:2|pid4:1
	static class RecomendMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if(StringUtil.isEmpty(line)) return;
			
			String[] arr = line.split("\t");
			context.write(new Text(arr[0]), new Text(arr[1]));
		}
		
	}
	
	static class RecomendReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException {
			
			Map<String,Integer> map = new HashMap<>();
			for(Text value :values){
				Integer count =  map.get(value.toString());
				count = (count == null) ? 1 : count+1;
				map.put(value.toString(), count);
			}
			StringBuffer sb = new StringBuffer();
			for(String k :map.keySet()){
				sb.append(k).append(":").append(map.get(k)).append("|");
			}
			sb.deleteCharAt(sb.length()-1);
			context.write(key, new Text(sb.toString()));
		}
		
	}
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, RecomendStep2MapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(RecomendStep2MapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(RecomendMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setPartitionerClass(HashPartitioner.class);
		//job.setCombinerClass(null);
		//job.setSortComparatorClass(cls);
		//job.setGroupingComparatorClass(cls);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(RecomendReducer.class);
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
				HOST+"/user/ehp/mapred/recomend/input",
				HOST+"/user/ehp/mapred/recomend/step2/output"
			};
		}
		
		System.exit(new RecomendStep2MapReduce().run(args));
	}
}

package com.hyman.mapred.recomend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.hsqldb.lib.StringUtil;

public class RecomendStep3MapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	//pid9    uid4    1
	//pid8    uid4    1
	// uid4	L	pid8
	// uid4	L	pid9
	static class ReadStep1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if(StringUtil.isEmpty(line)) return;
			
			String[] arr = line.split("\t");
			
			context.write(new Text(arr[1]),new Text("L\t"+arr[0]));
		}
	}
	//uid1    pid4:1|pid4:1|pid4:1|pid4:1
	static class ReadStep2Mapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if(StringUtil.isEmpty(line)) return;
			String[] arr = line.split("\t");
			context.write(new Text(arr[0]),new Text("R\t"+arr[1]));
		}
	}
	//uid1	L	pid1
	//uid1	L	pid2
	//uid1  R	pid4:1|pid4:1|pid4:1|pid4:1
	static class RecomendReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException {
			List<String> pidList = new ArrayList<>();
			String views = null;
			for(Text value :values){
				String[] arr = value.toString().split("\t");
				if(arr[0].equals("L")){
					pidList.add(arr[1]);
				}else{
					views = arr[1];
				}
			}
			
			for(String pid:pidList){
				context.write(new Text(pid), new Text(views));
			}
		}
		
	}
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, RecomendStep3MapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(RecomendStep3MapReduce.class);
		
	
		MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, ReadStep1Mapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, ReadStep2Mapper.class);
		
		
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
		Path path = new Path(args[2]);
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
				HOST+"/user/ehp/mapred/recomend/step1/output",
				HOST+"/user/ehp/mapred/recomend/step2/output",
				HOST+"/user/ehp/mapred/recomend/step3/output"
			};
		}
		
		System.exit(new RecomendStep3MapReduce().run(args));
	}
}

package com.hyman.hadoop.two;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobMain {

	public static void main(String[] args) throws Exception{
		if(args.length<2){
			System.err.println("Job:<in> <out>");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "child_grand-job");
		job.setJarByClass(JobMain.class);
		
		job.setMapperClass(SelfJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(SelfJoinReducer.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		
		Path outPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(outPath))
		{
			fs.delete(outPath,true);
			System.out.println("Out path is existed ,but has been deleted");
			System.exit(1);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true)?0:1);
	}

}

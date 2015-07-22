package com.hyman.hadoop.three;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
	
	private String input0;
	private String input1;
	private String output;

	@Override
	public int run(String[] args) throws Exception {
		
		configArgs(args);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JoinUser-job");
        job.setJarByClass(JobMain.class);

        MultipleInputs.addInputPath(job, new Path(input0),
                TextInputFormat.class, PhoneMapper.class);
        MultipleInputs.addInputPath(job, new Path(input1),
                TextInputFormat.class, InfoMapper.class);
        
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        
        
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserWritable.class);
        
        FileSystem fs = FileSystem.get(conf);
        
        Path path = new Path(output);
        if(fs.exists(path))
		{
			fs.delete(path,true);
			System.exit(1);
		}
		FileOutputFormat.setOutputPath(job, path);
        
        int exitcode = job.waitForCompletion(true) ? 0 : 1;
		return exitcode;
	}
	
	
	private void configArgs(String[] args){
		for(int i=0;i<args.length;i++){
			if("-i0".equals(args[i].trim())){
				input0 = StringUtils.trim(args[++i]);
			}
			if("-i1".equals(args[i].trim())){
				input1 = StringUtils.trim(args[++i]);
			}
			if("-o".equals(args[i].trim())){
				output = StringUtils.trim(args[++i]);
			}
		}
		checkParam();
	}
	
	
	
	private void checkParam() {
		if(StringUtils.isBlank(this.input0)){
			printTipsAndExit();
		}
		if(StringUtils.isBlank(this.input1)){
			printTipsAndExit();
		}
		if(StringUtils.isBlank(this.output)){
			printTipsAndExit();
		}
	}
	
	private void printTipsAndExit(){
		
		System.out.println("params:");
		System.out.println("\t-i0 input path1 is required");
		System.out.println("\t-i1 input path2 is required");
		System.out.println("\t-o out path is required");
		System.exit(2);
	}



	public static void main(String[] args) throws Exception{
		int exitcode = ToolRunner.run(new JobMain(), args);
        System.exit(exitcode);
	}

}

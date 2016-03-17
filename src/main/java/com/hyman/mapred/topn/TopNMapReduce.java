package com.hyman.mapred.topn;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopNMapReduce {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	static class TopNMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable>{

		int len ;
		int[] top;
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
						throws IOException, InterruptedException {
			String [] arr = value.toString().split(",");
			
			if(arr.length!=4) return;
			int payment = Integer.parseInt(arr[2]);
			putIntoTop(payment);
		}

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			len = Integer.parseInt(context.getConfiguration().get("N", "10"));
			top = new int[len+1];
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			for(int i=len;i>0;i--){
				context.write(new IntWritable(top[i]), NullWritable.get());
			}
		}
		
		private void putIntoTop(int payment){
			top[0] = payment;
			Arrays.sort(top);
		}
	}
	
	static class TopNReducer extends Reducer<IntWritable,NullWritable,IntWritable,IntWritable>{

		int len ;
		int[] top;
		
		@Override
		protected void reduce(IntWritable key, Iterable<NullWritable> values,Context context)
						throws IOException, InterruptedException {
			putIntoTop(key.get());
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			len = Integer.parseInt(context.getConfiguration().get("N", "10"));
			top = new int[len+1];
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			for(int i=len;i>0;i--){
				context.write(new IntWritable(len-i+1), new IntWritable(top[i]));
			}
		}
		
		private void putIntoTop(int payment){
			top[0] = payment;
			Arrays.sort(top);
		}
	}

	public static void main(String[] args) throws Exception {
		
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/topn/input",
				HOST+"/user/ehp/mapred/topn/output",
				"5"
			};
		}
		
		if (args.length < 3) {
			System.err.println("Usage: com.hyman.ehp.mapred.exp01.TopN inPath outPath N");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		conf.set("N", args[2]);
		
		Job job = Job.getInstance(conf);
		job.setJobName("TopN");
		job.setJarByClass(TopNMapReduce.class);
		job.setMapperClass(TopNMapper.class);
		
		job.setReducerClass(TopNReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

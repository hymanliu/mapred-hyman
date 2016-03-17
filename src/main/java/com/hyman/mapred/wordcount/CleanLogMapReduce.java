package com.hyman.mapred.wordcount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class CleanLogMapReduce {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		private static SimpleDateFormat dateformat = new SimpleDateFormat("[d/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
		
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//27.19.74.143 - - [30/May/2013:17:38:20 +0800] "GET /static/image/common/faq.gif HTTP/1.1" 200 1127
			
			String line = value.toString();
			String[] arr = line.split(" ");
			String ip = arr[0];
			String date =null;
			try {
				date = sdf.format(dateformat.parse(arr[3]));
			} catch (ParseException e) {
			}
			
			String reqType = arr[5].substring(1);
			String url = arr[6];
			String size = "0";
			if(arr.length>9){
				size = arr[9];
				if(!StringUtils.isNumeric(size)){
					size="0";
				}
			}
			Text outKey = new Text(ip+"\t"+date+"\t"+ reqType+"\t"+url+"\t"+size);
			
			context.write(outKey, NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
	/*	args = new String[]{
				"hdfs://hadoop-ehp.hyman.com:8020/user/ehp/mapred/cleanLog/input",
				"hdfs://hadoop-ehp.hyman.com:8020/user/ehp/mapred/cleanLog/output1"
				};*/
		
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.tasks", "0");
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: cleanLog <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf);
		job.setJobName("cleanLog");
		job.setJarByClass(CleanLogMapReduce.class);
		job.setMapperClass(MyMapper.class);
		
		//job.setReducerClass(Reducer.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}

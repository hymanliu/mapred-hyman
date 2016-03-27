package com.hyman.mapred.userlog;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TraficMapReduce extends Configured implements Tool {

	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Trafic Count");
		job.setJarByClass(TraficMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(TraficMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(TraficReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job, out);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	//27.19.74.143 - - [30/May/2013:17:38:20 +0800] "GET /static/image/common/faq.gif HTTP/1.1" 200 1127
	static class TraficMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\\s+");
			if(arr.length!=10) return;
				
				String ip = arr[0];
				String data = arr[9];
				if(data.matches("\\d+")){
					context.write(new Text(ip), new LongWritable(Long.parseLong(data)));
				}
			}
		}
	
	static class TraficReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
			long total = 0;
			for(LongWritable val :values){
				total+=val.get();
			}
			context.write(key, new LongWritable(total));
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/userlog/input",
				HOST+"/user/ehp/mapred/userlog/output"
			};
		}
		
		new TraficMapReduce().run(args);
		
	}
}

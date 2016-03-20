package com.hyman.mapred.search;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class QueryTimesFilterMapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	//date	user_id	search_content	sort_order	click_order	click_url
	//20111230000005	57375476989eea12893c0c3811607bcf	奇艺高清	1	1	http://www.qiyi.com/
	
	static class QueryTimesMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		static IntWritable val = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] arr = line.split("\t");
			context.write(new Text(arr[1]), val);
		}
	}
	
	static class QueryTimesReducer extends Reducer<Text, IntWritable,Text,IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for(IntWritable val :values)
			{
				count+=val.get();
			}
			context.write(key ,new IntWritable(count));
		}
	}
	
	static class FilterMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void map(Text key, IntWritable value,Context context)
				throws IOException, InterruptedException {
			
			if(value.get()>5){
				context.write(key,value);
			}	
		
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, QueryTimesFilterMapReduce.class.getName());
		job.setJarByClass(QueryTimesFilterMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		ChainMapper.addMapper(job, QueryTimesMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
		
		ChainReducer.setReducer(job, QueryTimesReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
		ChainReducer.addMapper(job, FilterMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
		
		job.setNumReduceTasks(1);
		
		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out,true);
		}
		FileOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/search/input",
				HOST+"/user/ehp/mapred/search/output"
			};
		}
		System.exit(new QueryTimesFilterMapReduce().run(args));
	}

}

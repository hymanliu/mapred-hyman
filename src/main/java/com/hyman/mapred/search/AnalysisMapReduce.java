package com.hyman.mapred.search;

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

public class AnalysisMapReduce extends Configured implements Tool{

	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	//date	user_id	search_content	sort_order	click_order	click_url
	//20111230000005	57375476989eea12893c0c3811607bcf	奇艺高清	1	1	http://www.qiyi.com/
	//count the line : sort_order = 1  and click_url=2
	static class AnalysicMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			String[] arr = line.split("\t");
			if("1".equals(arr[3]) && "2".equals(arr[4])){
				context.write(value, NullWritable.get());
			}
		}
	}
	
	static class AnalysicReducer extends Reducer<Text, NullWritable, IntWritable, NullWritable>{

		int total = 0;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			total = 0;
		}
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			total++;
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			context.write(new IntWritable(total), NullWritable.get());
		}
	} 
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, AnalysisMapReduce.class.getName());
		job.setJarByClass(AnalysisMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(AnalysicMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
//		job.setNumReduceTasks(0);
		
		job.setNumReduceTasks(1);
		
		job.setReducerClass(AnalysicReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out,true);
		}
		FileOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/search/input",
				HOST+"/user/ehp/mapred/search/output"
			};
		}
		System.exit(new AnalysisMapReduce().run(args));
	}
}

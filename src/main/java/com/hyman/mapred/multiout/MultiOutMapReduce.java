package com.hyman.mapred.multiout;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;


public class MultiOutMapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	static class MultiOutMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			if(line.length()>0){
				String arr[] = line.split(",");
				context.write(new Text(arr[0]), value);
			}
		}
	}
	
	static class MultiOutReducer extends Reducer<Text, Text, NullWritable, Text>{
		
		private MultipleOutputs<NullWritable, Text> mo = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mo = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			for(Text val : values){
				mo.write("SplitData", NullWritable.get(), val, key.toString()+"/");
				mo.write("AllData", NullWritable.get(), val);
			}
			
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mo.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, MultiOutMapReduce.class.getName());
		job.setJarByClass(MultiOutMapReduce.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MultiOutMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MultiOutReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		MultipleOutputs.addNamedOutput(job, "SplitData", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "AllData", TextOutputFormat.class, NullWritable.class, Text.class);
		
		FileSystem fs = FileSystem.get(conf);
		Path out = new Path(args[1]);
		if(fs.exists(out)){
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {

		if(args.length==0){
			args = new String[]{
				HOST+"/user/ehp/mapred/multiout/input",
				HOST+"/user/ehp/mapred/multiout/output"
			};
		}
		System.exit(new MultiOutMapReduce().run(args));
	}

}

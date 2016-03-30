package com.hyman.mapred.recomend;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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

public class RecomendStep4MapReduce extends Configured implements Tool {
	
	static final String HOST ="hdfs://hadoop110.hyman.com:8020";
	
	
	//pid9    pid1:5|pid1:5|pid1:5|pid1:5|pid1:5|pid1:5|pid1:5|pid1:5|pid1:5|pid1:5
	static class RecomendMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString().trim();
			if(StringUtil.isEmpty(line)) return;
			String[] arr = line.split("\t");
			context.write(new Text(arr[0]),new Text(arr[1]));
		}
	}
	//pid  pid4:1|pid4:1|pid4:1|pid4:1
	static class RecomendReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException {
			Map<String,Integer> viewMap = new HashMap<>();
			for(Text value :values){
				String[] arr = value.toString().split("\\|");
				for(String view : arr){
					String[] v = view.split(":");
					if(v[0].equals(key.toString())) continue;
					Integer times = viewMap.get(v[0]);
					times = (times==null) ? Integer.parseInt(v[1]):times + Integer.parseInt(v[1]);
					viewMap.put(v[0], times);
				}
			}
			List<ProductView> viewsList = new LinkedList<>();
			
			for(String pid : viewMap.keySet()){
				viewsList.add(new ProductView(pid,viewMap.get(pid)));
			}
			Collections.sort(viewsList);
			StringBuffer recomends = new StringBuffer();
			for(ProductView pv : viewsList){
				recomends.append(pv.getProductId()).append(":").append(pv.getTimes()).append("|");
			}
			recomends.deleteCharAt(recomends.length()-1);
			
			context.write(key, new Text(recomends.toString()));
		}
		
	}
	
	static class ProductView implements Comparable<ProductView>{
		
		private String productId;
		private Integer times;

		public ProductView(){}
		
		public ProductView(String productId, Integer times) {
			this.productId = productId;
			this.times = times;
		}

		public String getProductId() {
			return productId;
		}

		public void setProductId(String productId) {
			this.productId = productId;
		}

		public Integer getTimes() {
			return times;
		}

		public void setTimes(Integer times) {
			this.times = times;
		}

		@Override
		public int compareTo(ProductView o) {
			int cop = times - o.getTimes();
			if (cop != 0)
				return -cop;
			else
				return -productId.compareTo(o.productId );
		}
	}
	
	
	public int run(String[] args) throws Exception {
		
		//step 1: conf
		Configuration conf = new Configuration();
		//step 2: create job
		Job job = Job.getInstance(conf, RecomendStep4MapReduce.class.getName());
		//step 3: set jar by class
		job.setJarByClass(RecomendStep4MapReduce.class);
		
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(RecomendMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
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
				HOST+"/user/ehp/mapred/recomend/step3/output",
				HOST+"/user/ehp/mapred/recomend/step4/output"
			};
		}
		
		System.exit(new RecomendStep4MapReduce().run(args));
	}
}

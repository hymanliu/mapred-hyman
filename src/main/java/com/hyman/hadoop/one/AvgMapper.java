package com.hyman.hadoop.one;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AvgMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	
	/**
	 * userid  money
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString().trim();
		
		if(line.length()>0){
			String[] arr = line.split("\t");
			context.write(new Text(arr[0]), new IntWritable(Integer.parseInt(arr[1])));
		}
	}

	
}

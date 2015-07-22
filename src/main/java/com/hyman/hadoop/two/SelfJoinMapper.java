package com.hyman.hadoop.two;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelfJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		if(line.length()>0){
			String[] arr = line.split("\t");
			if(arr.length==2){
				context.write(new Text(arr[0].trim()), new Text("1,"+arr[1].trim()));
				context.write(new Text(arr[1].trim()), new Text("0,"+arr[0].trim()));
			}
		}
	}
	
	
}
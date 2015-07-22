package com.hyman.hadoop.three;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InfoMapper extends Mapper<LongWritable, Text, Text, UserWritable> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		if(line.length()>0){
			String[] arr = line.split("\t");
			context.write(new Text(arr[0]) , new UserWritable(arr[1]+"\t"+arr[2],1));
		}
	}

}

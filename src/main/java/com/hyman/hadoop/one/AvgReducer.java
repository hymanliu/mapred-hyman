package com.hyman.hadoop.one;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int len = 0;
		for(IntWritable val:values){
			sum+= val.get();
			len+=1;
		}
		
		context.write(key, new IntWritable(Math.round(sum/len)));
	}

}

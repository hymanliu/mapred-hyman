package com.hyman.hadoop.two;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SelfJoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		List<String> grandList = new ArrayList<String>();
		List<String> childList = new ArrayList<String>();
		
		for(Text val:values){
			String[] arr = val.toString().trim().split(",");
			if("0".endsWith(arr[0])){
				childList.add(arr[1]);
			}else
			{
				grandList.add(arr[1]);
			}
		}
		
		for(String child:childList){
			for(String grand:grandList){
				context.write(new Text(child), new Text(grand));
			}
		}
	}

}

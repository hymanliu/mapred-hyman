package com.hyman.hadoop.three;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, UserWritable, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<UserWritable> values,Context context)
			throws IOException, InterruptedException {
		
		User user = null;
		
		for(UserWritable u:values) {
			if(u.getType()==0){
				if(user == null){
					user = new User(key.toString(),u.getInfo());
				}
				else{
					user.setPhone(u.getInfo());
				}
			}
			else{
				String info = u.getInfo();
				String[] arr = info.split("\t");
				if(user == null){
					user = new User(key.toString(),arr[0],arr[1]);
				}
				else{
					user.setAge(arr[1]);
					user.setUsername(arr[0]);
				}
			}
		}
		context.write(NullWritable.get(), new Text(user.toString()));
	}
	
}

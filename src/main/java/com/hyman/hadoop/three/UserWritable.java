package com.hyman.hadoop.three;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserWritable implements Writable {
	
	private String info;
	private int type;
	
	public UserWritable(){}
	
	public UserWritable(String info,int type){
		this.info = info;
		this.type = type;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.info);
		out.writeInt(type);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.info = in.readUTF();
		this.type = in.readInt();
	}

	public String getInfo() {
		return info;
	}

	public int getType() {
		return type;
	}
}

package com.hyman.ehp.mapred.exp02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Join {
	
	
	static class EmployeeWritable implements Writable{
		
		private String deptNo ="";
		private String deptName="";
		private String loc="";
		
		private String empNo="";
		private String empName="";
		private String job="";
		private String leader="";
		private String date="";
		private String sal="";
		private String comm="";

		private int flag;
		
		public EmployeeWritable(){}
		public String getDeptNo() {
			return deptNo;
		}


		public void setDeptNo(String deptNo) {
			this.deptNo = deptNo;
		}


		public String getDeptName() {
			return deptName;
		}


		public void setDeptName(String deptName) {
			this.deptName = deptName;
		}


		public String getLoc() {
			return loc;
		}


		public void setLoc(String loc) {
			this.loc = loc;
		}


		public String getEmpNo() {
			return empNo;
		}


		public void setEmpNo(String empNo) {
			this.empNo = empNo;
		}


		public String getEmpName() {
			return empName;
		}


		public void setEmpName(String empName) {
			this.empName = empName;
		}


		public String getJob() {
			return job;
		}


		public void setJob(String job) {
			this.job = job;
		}


		public String getLeader() {
			return leader;
		}


		public void setLeader(String leader) {
			this.leader = leader;
		}


		public String getDate() {
			return date;
		}


		public void setDate(String date) {
			this.date = date;
		}


		public String getSal() {
			return sal;
		}


		public void setSal(String sal) {
			this.sal = sal;
		}


		public String getComm() {
			return comm;
		}


		public void setComm(String comm) {
			this.comm = comm;
		}


		public void setFlag(int flag) {
			this.flag = flag;
		}


		public EmployeeWritable(String deptNo,String deptName,String loc){
			this.deptNo = deptNo;
			this.deptName = deptName;
			this.loc = loc;
			this.flag = 2;
		}
								//7499	ALLEN	SALESMAN	7698	1981-2-20	1600.00	300.00	30
		public EmployeeWritable(String empNo,String empName,String job,String leader,String date,String  sal,String comm,String deptNo){
			this.empNo = empNo;
			this.empName = empName;
			this.job = job;
			this.leader = leader;
			this.date = date;
			this.sal = sal;
			this.comm = comm;
			this.deptNo = deptNo;
			this.flag = 1;
		}
		
		public void setDept(EmployeeWritable dept){
			this.deptName = dept.deptName;
			this.loc = dept.loc;
		}

		public int getFlag() {
			return flag;
		}

		public void write(DataOutput out) throws IOException {
			
			out.writeUTF(deptNo);
			out.writeUTF(deptName);
			out.writeUTF(loc);
			out.writeUTF(empNo);
			out.writeUTF(empName);
			out.writeUTF(job);
			out.writeUTF(leader);
			out.writeUTF(date);
			out.writeUTF(sal);
			out.writeUTF(comm);
			out.writeInt(flag);
		}

		public void readFields(DataInput in) throws IOException {
			deptNo = in.readUTF();
			deptName = in.readUTF();
			loc = in.readUTF();
			empNo = in.readUTF();
			empName = in.readUTF();
			job = in.readUTF();
			leader = in.readUTF();
			date = in.readUTF();
			sal = in.readUTF();
			comm = in.readUTF();
			flag = in.readInt();
			
		}

		@Override
		public String toString() {
			return  deptNo + "\t" + deptName + "\t" + loc + "\t" + empNo
					+ "\t" + empName + "\t" + job + "\t" + leader + "\t" + date + "\t" + sal
					+ "\t" + comm;
		}
		
		
	}
	

	static class JoinMapper extends Mapper<LongWritable,Text,Text,EmployeeWritable>{

		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
			EmployeeWritable emp = null;
			Text outKey = new Text();
			if(arr.length==3){
				outKey.set(arr[0]);
				emp = new EmployeeWritable(arr[0],arr[1],arr[2]);
			}
			else{
				outKey.set(arr[7]);
				emp = new EmployeeWritable(arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],arr[6],arr[7]);
			}
			context.write(outKey, emp);
		}
	}
	
	static class JoinReducer extends Reducer<Text,EmployeeWritable,Text,NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<EmployeeWritable> values,Context context)
						throws IOException, InterruptedException {

			EmployeeWritable dept  = null;
			List<EmployeeWritable> empList = new ArrayList<EmployeeWritable>();
			
			for(EmployeeWritable emp : values){
				if(emp.getFlag()==2){
					dept = emp;
				}
				else{
					empList.add(emp);
				}
			}
			
			for(EmployeeWritable emp : empList){
				emp.setDept(dept);
				context.write(new Text(emp.toString()), NullWritable.get());
			}
		}

		
	}
	
	public static void main(String[] args) throws Exception {
		
		if(args.length==0){
			args = new String[]{
				"hdfs://hadoop-ehp.hyman.com:8020/user/ehp/mapred/join/input",
				"hdfs://hadoop-ehp.hyman.com:8020/user/ehp/mapred/join/output",
			};
		}
		
		if (args.length < 2) {
			System.err.println("Usage: com.hyman.ehp.mapred.exp02.join inPath outPath");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Job job = Job.getInstance(conf);
		job.setJobName("Join");
		job.setJarByClass(Join.class);
		job.setMapperClass(JoinMapper.class);
		
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(EmployeeWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}

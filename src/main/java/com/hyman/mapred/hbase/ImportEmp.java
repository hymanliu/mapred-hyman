package com.hyman.mapred.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 参考 org.apache.hadoop.hbase.mapreduce.ImportTsv
 * org.apache.hadoop.hbase.mapreduce.TsvImporterMapper
 * @author Hyman
 */
public class ImportEmp  extends Configured implements Tool{
	
	public static final String COLUMN_FAMILY = "info";
	public static final String[] COLUMNS = new String[]{"rowkey","name","deptname","leader","joindate","sal","exp","deptno"};
	
	//7499    ALLEN   SALESMAN        7698    1981-2-20       1600.00 300.00  30
	static class ImportEmpMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		ImmutableBytesWritable outkey = new ImmutableBytesWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
						throws IOException, InterruptedException {
			
			String line = value.toString();
			
			//TODO validate data
			// .... 
			
			String[] fields = new String[8];
				StringTokenizer token = new StringTokenizer(line);
			int i = 0;
			while (token.hasMoreTokens()){
				fields[i++] = token.nextToken();
			}
			
			outkey.set(Bytes.toBytes(fields[0]));
			Put put = new Put(Bytes.toBytes(fields[0]));
			
			for(int index=1;index<8 ;index++){
				if(StringUtils.isNotEmpty(fields[index]))
					put.add(Bytes.toBytes(COLUMN_FAMILY),Bytes.toBytes(COLUMNS[index]), Bytes.toBytes(fields[index]));
			}
			
			context.write(outkey,put);
		}
		
	}

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

		
        job.setMapperClass(ImportEmpMapper.class);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        
        FileInputFormat.addInputPath(job, new Path(args[1]));
        
		TableMapReduceUtil.initTableReducerJob(
		  args[0],        // output table
		  null,    // reducer class
		  job//
		);
		
		job.setNumReduceTasks(0);   // at least one, adjust as required
		
        int exitcode = job.waitForCompletion(true) ? 0 : 1;
		return exitcode;
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = HBaseConfiguration.create();
		
		int exitcode = ToolRunner.run(//
				conf, //
				new ImportEmp(), //
				args//
			);
        System.exit(exitcode);
	}
}

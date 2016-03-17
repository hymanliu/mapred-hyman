package com.hyman.mapred.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CopyUser2Basic extends Configured implements Tool {
	
	static class TableReadMapper extends TableMapper<Text,Put>{
		
		private Text mapOutputKey = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)
						throws IOException, InterruptedException {
			
			String rowkey = Bytes.toString(key.get());
			// set map ouput key
			mapOutputKey.set(rowkey);
			
			//-----------------------------------
			// Create Put
			Put put = new Put(key.get());
			
			// iterator
			for(Cell cell : value.rawCells()){
				// add family: info
				if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
					// add column: name
						if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
							put.add(cell);
						}
					// add column: age
					if("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
						put.add(cell);
					}
				}
			}
			// context write
			context.write(mapOutputKey, put);
		}
		
		
	}
	
	static class TableWriteReducer extends TableReducer<Text,Put, ImmutableBytesWritable>{

		@Override
		protected void reduce(Text key, Iterable<Put> values, Context context)
				throws IOException, InterruptedException {
			
			for(Put put : values){
				context.write(null, put);
			}
		}
		
	}

	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        // set job
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs
		
		TableMapReduceUtil.initTableMapperJob(
		  "user",        // input table
		  scan,               // Scan instance to control CF and attribute selection
		  TableReadMapper.class,     // mapper class
		  Text.class,         // mapper output key
		  Put.class,  // mapper output value
		  job//
		 );
		
		TableMapReduceUtil.initTableReducerJob(
		  "basic",        // output table
		  TableWriteReducer.class,    // reducer class
		  job//
		);
		
		job.setNumReduceTasks(1);   // at least one, adjust as required
		
        int exitcode = job.waitForCompletion(true) ? 0 : 1;
		return exitcode;
	}
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = HBaseConfiguration.create();
		
		int exitcode = ToolRunner.run(//
				conf, //
				new CopyUser2Basic(), //
				args//
			);
        System.exit(exitcode);
	}
}

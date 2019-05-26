/*
File thông tin giao dịch sản ph input.txt có dạng như sau:
ID, Name, Math, Physic,Chem.
1. Tao file.
2. Thống kê điểm lớn nhất theo từng môn.
3. Đếm số lượng sinh viên đạt trên 6 điểm theo từng môn.

Giải;
2. function map()
Input: offsetRow, valOffset:Math,Physic,Chem.
Output: list(valOffset, 1).
	function reducer()
Input: valOffset, list(valOffset).
Output: list(valOffset, sum(valOffset).
 */
package practive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TranSactionDriver {
	protected static String inputFile = "/home/huongvt/Documents/BoDuLieu/de3.txt";
	protected static String outputFile = "/home/huongvt/Documents/output" + Math.random();
	public static void main(String[] args) {
		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(TranSactionDriver.class);

		// Set a name of the Job
		job_conf.setJobName("Word Count1");

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(TransactionMapper.class);
		job_conf.setReducerClass(TransactionReducer.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		
		//FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
		FileInputFormat.setInputPaths(job_conf, new Path(inputFile));
		FileOutputFormat.setOutputPath(job_conf, new Path(outputFile));
		
		my_client.setConf(job_conf);
		try {
			// Run the job 
			JobClient.runJob(job_conf);
	    	System.out.println("Transaction OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


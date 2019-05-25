/*File thong tin giao dich san pham input.txt co dang:
Transaction_Date, Product_Name, Price, City
Yeu cau: 
1. Tao 1 file input.txt co dang nhu tren lap lai 15 ban ghi.
2. Thong ke tong so lan giao dich cua moi san pham.
3. Tinh gia trung giao dich theo tung san pham.

Giai:
2.function map()
Input: offset, valueOffset.
OutPut: list(productName, 1).
function reducer()
Input: productName, list(productName)
Output: list(productName, sum(list(productName))
*/
package practive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TranSactionDriver {
	protected static String inputFile = "/home/huongvt/Documents/BoDuLieu/de1.txt";
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
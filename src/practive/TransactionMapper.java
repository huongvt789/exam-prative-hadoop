package practive;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private Text product = new Text();
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String[] array = value.toString().split(";");
		String[] company = array[1].split(",");
		
		for(int i=0;i<company.length;i++) {
			product.set(company[i]);
			output.collect(product, new IntWritable(1));
		}
	}
}
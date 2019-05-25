package practive;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text productName = new Text();
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String[] array = value.toString().split(",");
		String product = array[1];
		IntWritable price = new IntWritable(Integer.parseInt(array[2]));
		productName.set(product);
		output.collect(productName, price);
		}
}

package practive;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Text city = new Text();
	private Text product = new Text();
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] array = value.toString().split(" ");
		String nameCity = array[1];
		String productName = array[0];
		city.set(nameCity);
		product.set(productName);
		output.collect(city, product);
	}
}
package practive;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TransactionReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
		Text productName = t_key;
		int count = 0;
		int sum = 0;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			count++;
			IntWritable price = (IntWritable) values.next();
			sum = price.get();
		}
		System.out.println(sum/count);
		output.collect(productName, new DoubleWritable(sum/count));
	}
}

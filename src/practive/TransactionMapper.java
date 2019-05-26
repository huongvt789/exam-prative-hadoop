package practive;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

	public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private Text city = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] array = value.toString().split(" ");
			String nameCity = array[1];
			city.set(nameCity);
			output.collect(city, one);
			}
		}
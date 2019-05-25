package practive;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//		StringTokenizer itr = new StringTokenizer(value.toString());
//		while (itr.hasMoreTokens()) {
//			String valueOffset = itr.nextToken();
//			String list[] = valueOffset.split(",");
//			Text t = new Text(list[list.length-3]);
//			System.out.print(t);
//			output.collect(t, new IntWritable(1));
//			
			String[] array = value.toString().split(",");
			String city = array[1];
			System.out.print(city);
			word.set(city);
			output.collect(word, one);
		}
}
package practive;

	import java.io.IOException;
	import java.util.*;

	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapred.*;

	public class TransactionReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
			Text key = t_key;
			int count = 0;
			while (values.hasNext()) {
				double x = values.next().get();
				if(x > 6) count++;
			}
			output.collect(key, new DoubleWritable(count));
		}
	}

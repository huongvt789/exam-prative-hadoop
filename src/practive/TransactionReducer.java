package practive;

	import java.io.IOException;
	import java.util.*;

	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapred.*;

	public class TransactionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			Text key = t_key;
			String sum = "";
			while (values.hasNext()) {
				sum += values.next().toString() + ",";
			}
			output.collect(key, new Text(sum));
		}
	}

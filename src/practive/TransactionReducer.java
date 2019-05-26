package practive;

	import java.io.IOException;
	import java.util.*;

	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapred.*;

	public class TransactionReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
			Text key = t_key;
			double maxPoint = Double.NEGATIVE_INFINITY; //else: Double.Nagative_I...... (tim max)
			System.out.println(values);
			while (values.hasNext()) {
				// replace type of value with the actual type of our value
				DoubleWritable value = (DoubleWritable) values.next();
				double point = value.get();
				maxPoint = maxPoint > point ? maxPoint : point;
			}
			output.collect(key, new DoubleWritable(maxPoint));
		}
	}

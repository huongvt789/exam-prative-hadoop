package practive;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

	public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text nameSubject = new Text();
		private DoubleWritable point = new DoubleWritable();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String $delimiters = "\n";
			StringTokenizer itr = new StringTokenizer(value.toString(), $delimiters);
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String list[] = str.split(",");
				for (int i = 1; i <= 3; i++) {
					switch(i) {
						case 1:
							nameSubject.set(new Text("Math"));
							break;

						case 2:
							nameSubject.set(new Text("Physic"));
							break;

						case 3:
							nameSubject.set(new Text("Chemistry"));
							break;
					}
					point.set(Double.parseDouble(list[i + 1]));
					output.collect(nameSubject, point);
				}
			}
		}
	}
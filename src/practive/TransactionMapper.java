package practive;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Text companyName = new Text();
	private Text productId = new Text();
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String[] array = value.toString().split(";");
		String[] company = array[1].split(",");
		productId.set(array[0]);
		for(int i=0;i<company.length;i++) {
			companyName.set(company[i]);
			output.collect(companyName, productId);
		}
	}
}
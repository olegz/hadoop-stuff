package oz.poc.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable intWritable : values) {
			sum += intWritable.get();
		}
		System.out.println("Reducing sum: " + sum);
		context.write(key, new IntWritable(sum));
	}
}

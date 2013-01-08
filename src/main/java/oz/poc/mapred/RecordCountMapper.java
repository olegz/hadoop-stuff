package oz.poc.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RecordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static Text word = new Text("Compressed Records");
	private final static IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(word, one);
	}
}
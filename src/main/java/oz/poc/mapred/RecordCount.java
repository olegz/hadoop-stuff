package oz.poc.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.util.StringUtils;

public class RecordCount {
	
	public static class TokenizerMapper extends MapReduceBase implements
			Mapper<LongWritable, BytesWritable, Text, IntWritable> {
		
		private Text word = new Text("Record Count");

		@Override
		public void map(LongWritable key, BytesWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			String decompressed = this.decompressBOS(value.getBytes());
			StringTokenizer itr = new StringTokenizer(decompressed, "\n");
			output.collect(word, new IntWritable(itr.countTokens()));
		}
		
		private String decompressBOS(byte[] bytesIn) {
			try {
				ByteArrayInputStream bais = new ByteArrayInputStream(bytesIn);
				GZIPInputStream gzip = new GZIPInputStream(bais);
				byte[] bytes = new byte[64768];
				StringBuffer buffer = new StringBuffer();
				int length = 0;
				while (length > -1){
					length = gzip.read(bytes);
					if (length > -1) {
						buffer.append(new String(bytes, 0, length, "ISO-8859-1"));
					}			
				}
				return buffer.toString();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static class IntSumReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("######## Starting task #########");
		System.out.println("Arguments: " + Arrays.asList(args) + " " + args.length);
		String[] argumentsParsed = StringUtils.delimitedListToStringArray(args[0], ",");
		
		String user = argumentsParsed[0];
		final String inputPath = argumentsParsed[1];
		final String outputPath = argumentsParsed[2];
		final String nameNodeAddress = argumentsParsed[3];
		final String jobTrackerAddressAddress = argumentsParsed[4];
		final String splitSize = argumentsParsed[5];
		final String jar = argumentsParsed[6];
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
		ugi.doAs(new PrivilegedAction<Object>() {

			@Override
			public Object run() {
				try {
					
					JobConf conf = new JobConf("record count");
					conf.set("fs.default.name", nameNodeAddress);
					conf.set("mapred.job.tracker", jobTrackerAddressAddress);
					conf.set("mapred.max.split.size", splitSize);
					//mapred.max.split.size=536870912
					conf.setJar("file:" + jar);
					conf.setMapperClass(TokenizerMapper.class);
					conf.setInputFormat(SequenceFileInputFormat.class);
					conf.setCombinerClass(IntSumReducer.class);
					conf.setReducerClass(IntSumReducer.class);
					conf.setOutputKeyClass(Text.class);
					conf.setOutputValueClass(IntWritable.class);
					FileInputFormat.addInputPath(conf, new Path(inputPath));
					FileOutputFormat.setOutputPath(conf, new Path(outputPath));
					JobClient.runJob(conf);
				} catch (Exception e) {
					e.printStackTrace();
				}

				return null;
			}
		});

	}
}

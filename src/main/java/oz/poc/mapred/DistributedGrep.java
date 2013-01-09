package oz.poc.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.util.StringUtils;

public class DistributedGrep {
	
	public static class TokenizerMapper extends MapReduceBase implements
			Mapper<LongWritable, BytesWritable, Text, IntWritable> {
		
		//final MessageChannel wireTapChannel;
		
		final Pattern p = Pattern.compile("OLEG ZHURAKOUSKY");
		
		public TokenizerMapper() {
//			 GenericXmlApplicationContext context = new
//			 GenericXmlApplicationContext();
//			 context.load("classpath:wiretap-config-template.xml");
//			 Map<String, Object> configurator = new HashMap<String, Object>();
//			 configurator.put("host", "192.168.47.1");
//			 configurator.put("port", 55555);
//			 context.getBeanFactory().registerSingleton("configurator",
//			 configurator);
//			 context.refresh();
//			 wireTapChannel = context.getBean("wiretapChannel", MessageChannel.class);
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {	
			String decompressed = this.decompressBOS(value.getBytes());
			Matcher m = p.matcher(decompressed);
			while (m.find()) {
				//wireTapChannel.send(new GenericMessage<String>("found"));
				System.out.println("Found");
			}
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

	public static void main(String[] args) throws Exception {
		System.out.println("######## Starting task #########");
		System.out.println("Arguments: " + Arrays.asList(args) + " " + args.length);
		String[] argumentsParsed = StringUtils.delimitedListToStringArray(args[0], ",");
		
		String user = argumentsParsed[0];
		final String inputPath = argumentsParsed[1];
		//final String outputPath = argumentsParsed[2];
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
					conf.set("mapred.reduce.tasks", "0");
					conf.set("mapred.reduce.tasks", "0");
//					conf.set("mapred.tasktracker.map.tasks.maximum", "128");
//					conf.set("mapred.map.tasks", "64");
					conf.setJar("file:" + jar);
					conf.setMapperClass(TokenizerMapper.class);
					conf.setInputFormat(SequenceFileInputFormat.class);
					conf.setOutputFormat(NullOutputFormat.class);
					FileInputFormat.addInputPath(conf, new Path(inputPath));

					JobClient.runJob(conf);
				} catch (Exception e) {
					e.printStackTrace();
				}

				return null;
			}
		});

	}
}

package oz.poc.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordFinderMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static Text word = new Text("Compressed Records");
	private final static IntWritable one = new IntWritable(1);

	public RecordFinderMap() {
		// GenericXmlApplicationContext context = new
		// GenericXmlApplicationContext();
		// context.load("classpath:wiretap-config-template.xml");
		// Map<String, Object> configurator = new HashMap<String, Object>();
		// configurator.put("host", "192.168.47.1");
		// configurator.put("port", 55555);
		// context.getBeanFactory().registerSingleton("configurator",
		// configurator);
		// context.refresh();
		// wireTapChannel = context.getBean("wiretapChannel",
		// MessageChannel.class);
	}
	//public void map(Object key, BytesWritable value, Context context)
	//OutputCollector<Text, IntWritable> output, Reporter reporter
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException, InterruptedException {
		
		//byte[] bytes = value.getBytes();
		//output.collect(word, one);
		//new Exception().printStackTrace();
		//System.out.println("## " + value.getClass().getName());
		// String line = value.toString();
		// String decompressedString = decompress(line);
		// StringTokenizer tokenizer = new
		// StringTokenizer(decompressedString.trim(), "\n");
		// while (tokenizer.hasMoreTokens()) {
		// String nextToken = tokenizer.nextToken();
		// System.out.println("=> " + nextToken);
		// if (nextToken.contains("126.247.0.97")) {
		// //Thread.sleep(new Random().nextInt(5000));
		// //System.out.println(nextToken);
		// boolean sent = wireTapChannel.send(new
		// GenericMessage<String>(nextToken));
		// if (!sent){
		// throw new RuntimeException("Failed to send message");
		// }
		// //System.out.println("Found: ");
		// }
		// }
	}

	private static String decompress(byte[] compressedBytes) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
			GZIPInputStream gzip = new GZIPInputStream(bais);
			byte[] bytes = new byte[32768];
			int length = gzip.read(bytes);
			return new String(bytes, 0, length, "ISO-8859-1");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
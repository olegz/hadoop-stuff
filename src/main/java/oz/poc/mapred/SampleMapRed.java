package oz.poc.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.message.GenericMessage;

public class SampleMapRed extends Mapper<Object, Text, Text, IntWritable>{
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String decompressedString = decompress(line);
		StringTokenizer tokenizer = new StringTokenizer(decompressedString.trim(), "\n");
		while (tokenizer.hasMoreTokens()) {
			String nextToken = tokenizer.nextToken();
			if (nextToken.contains("Oleg and Tom") ){
				
			}
		}
	}
	private static String decompress(String value) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(
					Base64.decodeBase64(value.getBytes("ISO-8859-1")));
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
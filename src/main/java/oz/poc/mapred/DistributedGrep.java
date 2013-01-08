package oz.poc.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
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
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.message.GenericMessage;

public class DistributedGrep {
	
	public static class TokenizerMapper extends MapReduceBase implements
			Mapper<LongWritable, BytesWritable, Text, IntWritable> {
		
		final MessageChannel wireTapChannel;
		
		final Pattern p = Pattern.compile("OLEG ZHURAKOUSKY");
		
		public TokenizerMapper() {
			 GenericXmlApplicationContext context = new
			 GenericXmlApplicationContext();
			 context.load("classpath:wiretap-config-template.xml");
			 Map<String, Object> configurator = new HashMap<String, Object>();
			 configurator.put("host", "192.168.47.1");
			 configurator.put("port", 55555);
			 context.getBeanFactory().registerSingleton("configurator",
			 configurator);
			 context.refresh();
			 wireTapChannel = context.getBean("wiretapChannel", MessageChannel.class);
		}
		
		@Override
		public void map(LongWritable key, BytesWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {	
			String decompressed = this.decompressBOS(value.getBytes());
			Matcher m = p.matcher(decompressed);
			while (m.find()) {
				wireTapChannel.send(new GenericMessage<String>("found"));
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
		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser("hduser");
		ugi.doAs(new PrivilegedAction<Object>() {

			@Override
			public Object run() {
				try {
					// Configuration conf = new Configuration();

					JobConf conf = new JobConf("word count");
					conf.set("fs.default.name", "192.168.47.10:54310");
					conf.set("mapred.job.tracker", "192.168.47.10:54311");
					conf.set("mapred.max.split.size", "536870912");
					conf.set("mapred.reduce.tasks", "0");
					conf.set("dfs.data.dir", "${hadoop.tmp.dir}/dfs/data");
					conf.set("dfs.datanode.failed.volumes.tolerated", "1");
					
//					ClientProtocol namenode = DFSClient.createNamenode(conf);
//
//					LocatedBlocks blocks = namenode.getBlockLocations("/hduser/input/01_07_2013/192.168.15.130/cdr2.seq", 0, Long.MAX_VALUE);
//					List<LocatedBlock> locatedBlocks = blocks.getLocatedBlocks();
//					for (LocatedBlock locatedBlock : locatedBlocks) {
//						try {
//							LocalDataNodeInfo localDataNodeInfo = LocalDataNodeInfo.createLocalDataNodeInfo(conf, locatedBlock, 2000);
//							BlockLocalPathInfo pathinfo = localDataNodeInfo.getBlockLocalPathInfo();
//							System.out.println("==> " + pathinfo.getBlockPath());
//							//SequenceFileRecordReader<LongWritable, BytesWritable> sfrr = new SequenceFileRecordReader<LongWritable, BytesWritable>(conf, split);
//						} catch (Exception e) {
//							System.out.println(e.getMessage());
//						}
//						
//					}

//					//mapred.max.split.size=536870912
					conf.setJar("file:/Users/oleg/Documents/workspace/map-red-poc/build/libs/map-red-poc-1.3.jar");
					conf.setMapperClass(TokenizerMapper.class);
					conf.setInputFormat(SequenceFileInputFormat.class);
					conf.setOutputFormat(NullOutputFormat.class);
					
					FileInputFormat.addInputPath(conf, new Path("/hduser/output/01_08_2013/192.168.47.10/cdr.seq"));
					//FileInputFormat.addInputPath(conf, new Path("/hduser/input/01_07_2013/192.168.15.130/cdr2.seq"));
					JobClient.runJob(conf);
				} catch (Exception e) {
					e.printStackTrace();
				}

				return null;
			}
		});

	}
}

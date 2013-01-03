package oz.poc.main;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.springframework.util.StringUtils;

public class IngestTest {
	
	final private Random random = new Random();

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("######## Starting task #########");
		System.out.println("Arguments: " + Arrays.asList(args) + " " + args.length);
		String[] argumentsParsed = StringUtils.delimitedListToStringArray(args[0], ",");
		
		final int iterations = Integer.parseInt(argumentsParsed[0]);
		final int bufferSize = Integer.parseInt(argumentsParsed[1]);
		final String blockSize = argumentsParsed[2];
		final String strUri = argumentsParsed[3];
		final String targetPath = argumentsParsed[4];
		final String sourcePath = argumentsParsed[5];
		final String user = argumentsParsed[6];
		final int threadPool = Integer.parseInt(argumentsParsed[7]);
		final boolean blockCompression = Boolean.getBoolean(argumentsParsed[8]);
		
		//new IngestTest().toHDFS(1250, 1000, "536870912", "hdfs://192.168.47.10:54310", "/hduser/output/", "source/small-source.txt", "hduser", 4, false);
		new IngestTest().toHDFS(iterations, bufferSize, blockSize, strUri, targetPath, sourcePath, user, threadPool, blockCompression);
	}

	public void toHDFS(int iterations, final int bufferSize, String hdfsBlockSize, String targetUri, String targetPath, String sourcePath, String user, int threadPool, boolean blockCompression) throws Exception {
		DateFormat dateFormat = new SimpleDateFormat("MM_dd_yyyy");
		Calendar cal = Calendar.getInstance();
		
		ExecutorService compressingExecutor = Executors.newFixedThreadPool(threadPool);
		final ExecutorService writingExecutor = Executors.newSingleThreadExecutor();
		
		final InetAddress localHost = InetAddress.getLocalHost();
		Configuration configuration = new Configuration();
		configuration.set("dfs.block.size", hdfsBlockSize);// play around with this number (in bytes)
		FileSystem fs = FileSystem.get(new URI(targetUri), configuration, user);
		Path outFilePath = new Path(targetPath + "/" + dateFormat.format(cal.getTime()) + "/" + localHost.getHostAddress() + "/cdr.seq");
		SequenceFile.CompressionType compType = CompressionType.NONE;
		if (blockCompression){
			compType = CompressionType.BLOCK;
		}
		
		final SequenceFile.Writer writer = SequenceFile.createWriter(fs, configuration, outFilePath, LongWritable.class, ImmutableBytesWritable.class, compType);
		
		final LongWritable key = new LongWritable();
		
		final ArrayBlockingQueue<ImmutableBytesWritable> recordsToBeFlushedQueue = new ArrayBlockingQueue<ImmutableBytesWritable>(100);
		
		writingExecutor.execute(new Runnable() {
			
			@Override
			public void run() {
				try {
					ImmutableBytesWritable compressedBytes;
					int i = 1;
					long startTime = System.currentTimeMillis();
					while ((compressedBytes = recordsToBeFlushedQueue.poll(10000, TimeUnit.MILLISECONDS)) != null){
						writer.append(key, compressedBytes);
						if (i%1000 == 0){
							long stopTime = System.currentTimeMillis();
							System.out.println(i);
							System.out.println(localHost + " - Written " + (i*bufferSize) + " records in " + (stopTime - startTime) + " milliseconds");
							startTime = System.currentTimeMillis();
						}
						i++;
					}	
					System.out.println("I: " + i);
				} catch (Exception e) {
					//e.printStackTrace();
				}			
			}
		});
		
		System.out.println("Starting");
		
		for (int i = 0; i < iterations; i++) {
			final BufferedReader br = new BufferedReader(new FileReader(sourcePath));
			StringBuffer buffer = new StringBuffer(bufferSize * 230);
			int counter = 0;
			
			String line;
			while ((line = br.readLine()) != null) {
				buffer.append(line);
				counter++;
				this.delay();
				
				if (counter == bufferSize){
					counter = 0;
					buffer.trimToSize();
					final byte[] bytesToCompress = buffer.substring(0, buffer.capacity()).getBytes();
					compressingExecutor.execute(new Runnable() {		
						@Override
						public void run() {
							byte[] compressedBytes = compressBOS(bytesToCompress);
							try {
								recordsToBeFlushedQueue.offer(new ImmutableBytesWritable(compressedBytes), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
							} catch (Exception e) {
								e.printStackTrace();
							}
							
						}
					});
					buffer = new StringBuffer(bufferSize * 230);
				}
			}
			
			br.close();
		}
		
		compressingExecutor.shutdown();
		while (recordsToBeFlushedQueue.size() > 0){
			Thread.sleep(1000);
		}
		writingExecutor.shutdown();
	}
	
	private byte[] compressBOS(byte[] inputData) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(inputData);
			gzip.close();
			byte[] bytes = out.toByteArray();
			return bytes;
		} catch (Exception e) {
			throw new IllegalStateException("Failed to compress data", e);
		}	
	}
	
	private void delay() throws Exception {
		int i = random.nextInt(200);
		if (i%200 == 0){
			Thread.sleep(random.nextInt(1));
		}
	}
}

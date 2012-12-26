package oz.poc.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class CompressedSequenceFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		System.out.println("######## Starting task #########");
		System.out.println("Arguments: " + Arrays.asList(args) + " " + args.length);
		String arguments = Arrays.asList(args).get(0);
		String[] argumentsParsed = StringUtils.delimitedListToStringArray(arguments, ",");
		
		String methodName = argumentsParsed[0];
		
		CompressedSequenceFile testHarness = new CompressedSequenceFile();
		if (methodName.equalsIgnoreCase("prepareFile")){
			int value = Integer.parseInt(argumentsParsed[1]);
			String path = argumentsParsed[2];
			testHarness.prepareFile(value, path);
		}
		else if (methodName.equalsIgnoreCase("toHDFS")){
			int sourceRecordCount = Integer.parseInt(argumentsParsed[2]);
			int bufferSize = Integer.parseInt(argumentsParsed[3]);
			int blockSize = Integer.parseInt(argumentsParsed[4]);
			String uri = argumentsParsed[5];
			String user = argumentsParsed[6];
			String pathToHdfsFile = argumentsParsed[7];
			String sourcePath = argumentsParsed[8];
			int threadPool = Integer.parseInt(argumentsParsed[9]);
			
			boolean blockCompression = Boolean.getBoolean(argumentsParsed[10]);
			
			testHarness.toHDFS(sourceRecordCount, bufferSize, blockSize, uri, user, pathToHdfsFile, sourcePath, threadPool, blockCompression);
		}
		
		
	}
	
	public void prepareFile(int value, String path) throws Exception{
		BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		for (int i = 0; i < value; i++) {
			BufferedReader br = new BufferedReader(new FileReader("source/small-source.txt"));
			for (int j = 0; j < 80000; j++) {
				bw.write(br.readLine() + "\n");		
			}
			br.close();
		}
		System.out.println("Generated " + (80000*value) + " records file: ");
		bw.close();
	}
	
	public void toHDFS(int sourceRecordCount, int bufferSize, int blockSize, String uri, String user, String pathToHdfsFile, String sourcePath, int threadPool, boolean blockCompression) throws Exception {

		Assert.isTrue(sourceRecordCount % bufferSize == 0); // make sure its divisible without the remainder
		final int outerLoop = sourceRecordCount / bufferSize;
		final CountDownLatch latch = new CountDownLatch(outerLoop);
		
		final ExecutorService executor = Executors.newFixedThreadPool(threadPool);

		Configuration configuration = new Configuration();
		configuration.set("dfs.block.size", blockSize+"");// play around with this number (in bytes)
		FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
		Path outFilePath = new Path(pathToHdfsFile);
		SequenceFile.CompressionType compType = CompressionType.NONE;
		if (blockCompression){
			compType = CompressionType.BLOCK;
		}
		final SequenceFile.Writer writer = SequenceFile.createWriter(fs, configuration, outFilePath, IntWritable.class, ImmutableBytesWritable.class, compType);
		
		final IntWritable key = new IntWritable();
		
		final BufferedReader br = new BufferedReader(new FileReader(sourcePath));
		
		final ArrayBlockingQueue<ImmutableBytesWritable> recordQueue = new ArrayBlockingQueue<ImmutableBytesWritable>(outerLoop);
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				for (int i = 0; i < outerLoop; i++) {
					try {
						final ImmutableBytesWritable compressedBytes = recordQueue.poll(1000, TimeUnit.MILLISECONDS);
						writer.append(key, compressedBytes);
						
					} catch (Exception e) {
						e.printStackTrace();
					} 
					latch.countDown();
				}
			}
		});
		
		System.out.println("Starting");
		long start = System.currentTimeMillis();
		for (int i = 0; i < outerLoop; i++) {
			StringBuffer buffer = new StringBuffer(bufferSize * 230);
			for (int j = 0; j < bufferSize; j++) {
				String line = br.readLine();
				buffer.append(line);
				buffer.append("\n");
			}
			
			buffer.trimToSize();
			final byte[] bytesToCompress = buffer.substring(0, buffer.capacity()).getBytes();
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						byte[] compressedBytes = compressBOS(bytesToCompress);
						recordQueue.offer(new ImmutableBytesWritable(compressedBytes));
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			});
			
		}

		latch.await();
		long stop = System.currentTimeMillis();
		System.out.println("Compressed and written " + sourceRecordCount + " records in " + (stop - start) + " milliseconds");
		writer.close();
		br.close();
		executor.shutdownNow();
	}
	
	private byte[] compressBOS(byte[] inputData) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out, 8192);
		gzip.write(inputData);
		gzip.close();
		byte[] bytes = out.toByteArray();
		return bytes;
	}

}

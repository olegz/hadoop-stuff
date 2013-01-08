package oz.poc.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.junit.Test;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

public class TailFTest {
	
	
	
	
	@Test
	public void testReadSequenceFile() throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"), configuration, "hduser");
		Path inFilePath = new Path("/hduser/input/01_07_2013/192.168.15.130/cdr.seq");
		
		BytesWritable value = new BytesWritable();
		LongWritable key = new LongWritable();
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, inFilePath, configuration);
			while (reader.next(key, value)) {
				byte[] bytes = value.getBytes();
				System.out.println(key);
				System.out.println(bytes.length);
				//System.out.println(this.decompressBOS(bytes));
			}
		} 
		finally {
			IOUtils.closeStream(reader); 
		}
	}
	
	/**
	 * 0. RUN THIS BEFORE ANYTHING
	 * @throws Exception
	 */
	@Test
	public void prepSourceFile() throws Exception {
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("source/source.txt"));
		for (int i = 0; i < 125; i++) {
			BufferedReader br = new BufferedReader(new FileReader("source/small-source.txt"));
			for (int j = 0; j < 80000; j++) {
				bw.write(br.readLine() + "\n");		
			}
			br.close();
		}
		bw.close();
	}
	
	/**
	 * 1.
	 * Will establish the benchmark of how long does it take to write a 10M rec (~230 bytes each)
	 * to HDFS. No compression or other optimizations
	 * @throws Exception
	 */
	@Test
	public void writeToHDFSUncompressedFile() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("dfs.block.size", "134217728");// play around with this number (in bytes)
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"), configuration, "hduser");
		Path outFilePath = new Path("/hduser/input/words.txt");
		OutputStream outFile = fs.create(outFilePath);

		BufferedReader br = new BufferedReader(new FileReader("source/small-source.txt"));
		long start = System.currentTimeMillis();
		System.out.println("Starting");
		for (int i = 0; i < 1000; i++) {
			String line = br.readLine();
			if (line == null){
				break;
			}
			outFile.write((line+"\n").getBytes());
		}
		long stop = System.currentTimeMillis();
		System.out.println("Written " + 10000000 + " records in " + (stop - start) + " milliseconds");
		outFile.close();
		br.close();
	}
	
	@Test
	public void writeToHDFSCompressedFileOneThread() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("dfs.block.size", "134217728");// play around with this number (in bytes)
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"), configuration, "hduser");
		Path outFilePath = new Path("/hduser/input/compressed.txt");
		OutputStream outFile = fs.create(outFilePath);

		BufferedReader br = new BufferedReader(new FileReader("source/source.txt"));
		long start = System.currentTimeMillis();
		System.out.println("Starting");
		
		for (int i = 0; i < 10000; i++) {
			StringBuffer buffer = new StringBuffer();
			for (int j = 0; j < 1000; j++) {
				String line = br.readLine();
				buffer.append(line);
				buffer.append("\n");
			}
			String compressedRecord = this.compressRecord(buffer.toString()) + "\n";
			outFile.write(compressedRecord.getBytes());
		}
		long stop = System.currentTimeMillis();
		System.out.println("Written " + 10000000 + " records in " + (stop - start) + " milliseconds");
		outFile.close();
		br.close();
	}
	
	/**
	 * 2.
	 * Will demonstrate the improvements of writing the same file as above but as chunks of compressed data.
	 * Current configuration buffers 1000 records at the time. However at the client we were buffering 
	 * 200 records at the time. So its nice to play with both numbers.
	 * Also, the larger the chunk of compressed data the resulting file takes less space.
	 * For example the resulting file with chunks of 200 records per chunk takes more space then the same file with 1000
	 * records per chunk. However one must realized that the entire chunk will be read at once and sent to a MR task, so the chunk size 
	 * must be selected relative to the complexity of the MR task that will be processing.
	 * @throws Exception
	 */
	@Test
	public void writeToHDFSCompressedFile() throws Exception {
		int sourceRecords = 10000000;
		int bufferSize = 1000;
		Assert.isTrue(sourceRecords % bufferSize == 0); // make sure its divisible without the remainder
		final int outerLoop = sourceRecords / bufferSize;
		final CountDownLatch latch = new CountDownLatch(outerLoop);
		
		ExecutorService executor = Executors.newFixedThreadPool(8);

		//final FileOutputStream fos = new FileOutputStream(new File("source/compressed.txt"));
		Configuration configuration = new Configuration();
		configuration.set("dfs.block.size", "134217728");// play around with this number (in bytes)
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"), configuration, "hduser");
		Path outFilePath = new Path("/hduser/input/compressed.txt");
		final OutputStream fos = fs.create(outFilePath);
		
		
		final BufferedReader br = new BufferedReader(new FileReader("source/source.txt"));
		
		final ArrayBlockingQueue<String> recordQueue = new ArrayBlockingQueue<String>(outerLoop);
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				for (int i = 0; i < outerLoop; i++) {
					try {
						fos.write(recordQueue.poll(1000, TimeUnit.MILLISECONDS).getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						latch.countDown();
					}
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
			final String bufferString = buffer.toString();
			
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						String compressedRecord = compressRecord(bufferString) + "\n";
						recordQueue.offer(compressedRecord);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			});
			
		}

		latch.await();
		long stop = System.currentTimeMillis();
		System.out.println("Compressed and written " + sourceRecords + " records in " + (stop - start) + " milliseconds");
		fos.close();
		br.close();
	}

	/**
	 * 3.
	 * The previous tests shows the dramatic improvement in data ingest if such data comes as compressed chunks.
	 * However the profiling numbers included the time it takes to compress data and even though multiple threads were used.
	 * Now, pretend you have a separate process that reads the source file and generates compressed data so your ingest process 
	 * never have to deal with compression of any kind nd simply sinks data to HDFS, so this test simply reads the same source file and 
	 * generates its compressed euivalent with the same 1000 record chunks. 
	 * @throws Exception
	 */
	@Test
	public void generateCompressedFile() throws Exception {
		int sourceRecords = 10000000;
		int bufferSize = 1000;
		Assert.isTrue(sourceRecords % bufferSize == 0); // make sure its divisible without the remainder
		final int outerLoop = sourceRecords / bufferSize;
		final CountDownLatch latch = new CountDownLatch(outerLoop);
		
		ExecutorService executor = Executors.newFixedThreadPool(8);

		final FileOutputStream fos = new FileOutputStream(new File("source/compressed.txt"));
		final BufferedReader br = new BufferedReader(new FileReader("source/source.txt"));
		
		final ArrayBlockingQueue<String> recordQueue = new ArrayBlockingQueue<String>(outerLoop);
		executor.execute(new Runnable() {
			
			@Override
			public void run() {
				for (int i = 0; i < outerLoop; i++) {
					try {
						fos.write(recordQueue.poll(1000, TimeUnit.MILLISECONDS).getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						latch.countDown();
					}
				}
			}
		});
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < outerLoop; i++) {
			StringBuffer buffer = new StringBuffer(bufferSize * 230);
			for (int j = 0; j < bufferSize; j++) {
				String line = br.readLine();
				buffer.append(line + "\n");
			}
			final String bufferString = buffer.toString();
			
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						String compressedRecord = compressRecord(bufferString) + "\n";
						recordQueue.offer(compressedRecord);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			});
			
		}

		latch.await();
		long stop = System.currentTimeMillis();
		System.out.println("Compressed and written " + sourceRecords + " records in " + (stop - start) + " milliseconds");
		fos.close();
		br.close();
	}

	/**
	 * 4. 
	 * This test capitalizes on the improvements described in the previous test (3) but also goes to the heart of the client use case which is
	 * "parallel ingest" where the system consists of multiple writers. So this test takes the compressed version of the file produced 
	 * by the previous test, spuns off multiple threads where each thread represents a writer (a device in client's case) and 
	 * copies the compressed file. The amount of result files is equal to the amount of threads and the amount of data each file represents is still 10M
	 * so, with 10 threads the amount of data in the target files will represent 100M records and so on.
	 * Currently I have 26 threads which is the peek of what I can do on my machine until things start to break due to the resource starvation.
	 * However I do have a very powerful machine so I'd recommend to start slow. May be 10 threads to start with.
	 * @throws Exception
	 */
	@Test
	public void writeCompressedFileToHdfsAsMultipleDevicesAsync() throws Exception {
		int devices = 26;
		ExecutorService executor = Executors.newFixedThreadPool(devices);
		final CountDownLatch latch = new CountDownLatch(devices);

		Configuration configuration = new Configuration();
		configuration.set("dfs.block.size", "134217728");// play around with this number (in bytes)
		final FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"), configuration, "hduser");

		long start = System.currentTimeMillis();
		for (int i = 0; i < devices; i++) {
			final int I = i;
			executor.execute(new Runnable() {

				@Override
				public void run() {
					try {
						Path outFilePath = new Path("/hduser/input/compressed-" + I + ".txt");
						final OutputStream outFile = fs.create(outFilePath);
						File att = new File("source/compressed.txt");
						final BufferedReader br = new BufferedReader(new FileReader(att));
						System.out.println("Starting");
						for (int i = 0; i < 10000; i++) { // 10000 compressed record (10000 * 1000)
							String line = br.readLine();
							outFile.write((line + "\n").getBytes());
						}
						outFile.close();
						br.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
					latch.countDown();
				}
			});
		}
		latch.await();
		long stop = System.currentTimeMillis();
		System.out.println("Read and wrote " + (10000000 * devices) + " records in " + (stop - start) + " milliseconds");

	}
	
	private byte[] compressBOS(byte[] inputData) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out, 8192);
		gzip.write(inputData);
		gzip.close();
		byte[] bytes = out.toByteArray();
		//System.out.println("Unencoded length: " + bytes.length);
		return bytes;
//		String encoded = new String(Base64.encodeBase64(bytes));
//		System.out.println("Encoded length: " + encoded.length());
//		return encoded;
	}
	
	private static String decompressBOS(byte[] bytesIn) {
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
			e.printStackTrace();
		}
		return null;
	}

	private String compressRecord(String record) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out, 8192);
		gzip.write(record.getBytes("ISO-8859-1"));
		gzip.close();
		byte[] bytes = out.toByteArray();
		//System.out.println("Unencoded length: " + bytes.length);
		String encoded = new String(Base64.encodeBase64(bytes));
		//System.out.println("Encoded length: " + encoded.length());
		return encoded;
	}

}

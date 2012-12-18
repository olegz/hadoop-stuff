package hadoop.playground;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsReadWriteSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		new HdfsReadWriteSample().read();
		//new HdfsReadWriteSample().writeSingleRecordSingleThread();
		// new HdfsReadWriteSample().writeSingleRecordTwoThreads();
		// new HdfsReadWriteSample().writeSingleRecordTwoThreadsTwoFiles();
		// new HdfsReadWriteSample().writeSingleRecordFourThreadsFourFiles();
		// new HdfsReadWriteSample().write100RecordsSingleThread();

	}

	/**
	 * Will read file(s) from /user/hduser/input/ directory of HDFS
	 */
	// Make sure to point to the right Hadoop instance (host:port) line 38 and
	// 58
	// Assumes there is a file(s) in /user/hduser/input/ directory of HDFS
	// Also assume hadoop is running as 'hduser'. If not change the user name of
	// the hadoop process on line 30
	public void read() throws Exception {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"),
				configuration, "hduser");
		Path inFile = new Path("/hduser/input/cdr.txt");
		System.out.println(fs.exists(inFile));
		FSDataInputStream in = fs.open(inFile);

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		while ((strLine = br.readLine()) != null) {
			// Print the content on the console
			System.out.println(strLine.length());
			String decompressed = this.decompress(strLine);
			System.out.println(decompressed);
			StringTokenizer tokenizer = new StringTokenizer(decompressed.trim(), "\n");
			while (tokenizer.hasMoreTokens()) {
				System.out.println("token: " + tokenizer.nextToken());
			}
			
		}

		// byte[] buffer = new byte[1024];
		// int bytesRead = 0;
		// while ((bytesRead = in.read(buffer)) > 0) {
		// System.out.println(new String(buffer));
		// }
		br.close();
	}
	
	private  String decompress(String value) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(
					Base64.decodeBase64(value.getBytes()));
			GZIPInputStream gzip = new GZIPInputStream(bais);
			byte[] bytes = new byte[32768];
			int length = gzip.read(bytes);
			return new String(bytes, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Will write 10 Messages to /user/hduser/output/messages.txt of HDFS in
	 * JSON format
	 */
	// Assumes /user/hduser/output/ of HDFS does not exists
	// Also assume hadoop is running as 'hduser'. If not change the user name of
	// the hadoop process on line 58
	public void writeSingleRecordSingleThread() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(4);

		// Configuration configuration = new Configuration();
		// FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"),
		// configuration, "hduser");
		//
		// Path outFilePath = new Path("/hduser/input/cdr");
		// final OutputStream outFile = fs.create(outFilePath);
		final FileOutputStream outFile = new FileOutputStream(new File(
				"logs/largeFile.txt"));
		final String cdr = "1,17325551212,15,20000207062812,21060207062815,2000020706283030,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1"
				+ ",1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0,16,192.168.47.11,192.168.11.64,4,1,1,1,1,1,1,,0,016,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0";

		// final String cdr2 =
		// "1,17325551212,15,20000207062812,21060207062815,2000020706283030,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1"
		// +
		// ",1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0,16,192.168.47.10,192.168.10.64,4,1,1,1,1,1,1,,0,016,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0";

		System.out.println("Record length: " + cdr.length());

		int iterations = 100000;
		final CountDownLatch latch = new CountDownLatch(iterations);

		long start = System.currentTimeMillis();

		for (int i = 0; i < iterations; i++) {

			executor.execute(new Runnable() {

				@Override
				public void run() {
					try {
						StringBuffer buffer = new StringBuffer();
						for (int j = 0; j < 200; j++) {

							buffer.append(cdr + "\n");

						}
						String compressedRecord = compressRecord(buffer
								.toString()) + "\n";
						outFile.write(compressedRecord.getBytes());

						latch.countDown();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		latch.await();
		long stop = System.currentTimeMillis();
		System.out.println("Written 1000000 records in " + (stop - start)
				+ " milliseconds");

		outFile.close();
	}

	private String compressRecord(String record) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(record.getBytes("ISO-8859-1"));
		gzip.close();
		return new String(Base64.encodeBase64(out.toByteArray()));
	}

	public void write100RecordsSingleThread() throws Exception {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"),
				configuration, "hduser");

		Path outFilePath = new Path("/hduser/input/cdr.txt");
		final FSDataOutputStream outFile = fs.create(outFilePath);
		String cdr = "1,17325551212,15,20000207062812,21060207062815,2000020706283030,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1"
				+ ",1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,016,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0";

		System.out.println("Record length: " + cdr.length());
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			StringBuffer buffer = new StringBuffer();
			for (int j = 0; j < 100; j++) {
				buffer.append(cdr);
			}
			outFile.write(buffer.toString().getBytes());
		}
		long stop = System.currentTimeMillis();
		System.out.println("Written 1000000 records in " + (stop - start)
				+ " milliseconds");
		outFile.close();
	}

	public void writeSingleRecordTwoThreads() throws Exception {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"),
				configuration, "hduser");

		Path outFilePath = new Path("/hduser/input/cdr.txt");
		final FSDataOutputStream outFile = fs.create(outFilePath);

		final String cdr = "1,17325551212,15,20000207062812,21060207062815,2000020706283030,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1"
				+ ",1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,016,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0";

		System.out.println("Record length: " + cdr.length());
		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 500000; i++) {
					try {
						outFile.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 500000; i++) {
					try {
						outFile.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread[] threads = new Thread[] { t1, t2 };

		long start = System.currentTimeMillis();
		for (Thread thread : threads) {
			thread.start();
		}
		for (Thread thread : threads) {
			thread.join();
		}

		long stop = System.currentTimeMillis();
		System.out.println("Written 1000000 records in " + (stop - start)
				+ " milliseconds");
		outFile.close();

	}

	public void writeSingleRecordTwoThreadsTwoFiles() throws Exception {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"),
				configuration, "hduser");

		Path outFilePath1 = new Path("/hduser/input/cdr1.txt");
		Path outFilePath2 = new Path("/hduser/input/cdr2.txt");
		final FSDataOutputStream outFile1 = fs.create(outFilePath1);
		final FSDataOutputStream outFile2 = fs.create(outFilePath2);

		final String cdr = "1,17325551212,15,20000207062812,21060207062815,2000020706283030,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1"
				+ ",1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,016,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0";

		System.out.println("Record length: " + cdr.length());
		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 500000; i++) {
					try {
						outFile1.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 500000; i++) {
					try {
						outFile2.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread[] threads = new Thread[] { t1, t2 };

		long start = System.currentTimeMillis();
		for (Thread thread : threads) {
			thread.start();
		}
		for (Thread thread : threads) {
			thread.join();
		}

		long stop = System.currentTimeMillis();
		System.out.println("Written 1000000 records in " + (stop - start)
				+ " milliseconds");
		outFile1.close();
		outFile2.close();
	}

	public void writeSingleRecordFourThreadsFourFiles() throws Exception {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.47.10:54310"),
				configuration, "hduser");

		Path outFilePath1 = new Path("/hduser/input/cdr1.txt");
		Path outFilePath2 = new Path("/hduser/input/cdr2.txt");
		Path outFilePath3 = new Path("/hduser/input/cdr3.txt");
		Path outFilePath4 = new Path("/hduser/input/cdr4.txt");
		final FSDataOutputStream outFile1 = fs.create(outFilePath1);
		final FSDataOutputStream outFile2 = fs.create(outFilePath2);
		final FSDataOutputStream outFile3 = fs.create(outFilePath3);
		final FSDataOutputStream outFile4 = fs.create(outFilePath4);

		final String cdr = "1,17325551212,15,20000207062812,21060207062815,2000020706283030,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1"
				+ ",1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0,16,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,016,208.226.140.57,192.168.10.64,4,1,1,1,1,1,1,,0,0";

		System.out.println("Record length: " + cdr.length());
		Thread t1 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 250000; i++) {
					try {
						outFile1.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread t2 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 250000; i++) {
					try {
						outFile2.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread t3 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 250000; i++) {
					try {
						outFile3.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread t4 = new Thread(new Runnable() {

			@Override
			public void run() {
				for (int i = 0; i < 250000; i++) {
					try {
						outFile4.write(cdr.getBytes());
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}
		});
		Thread[] threads = new Thread[] { t1, t2, t3, t4 };

		long start = System.currentTimeMillis();
		for (Thread thread : threads) {
			thread.start();
		}
		for (Thread thread : threads) {
			thread.join();
		}

		long stop = System.currentTimeMillis();
		System.out.println("Written 1000000 records in " + (stop - start)
				+ " milliseconds");
		outFile1.close();
		outFile2.close();
		outFile3.close();
		outFile4.close();
	}
}

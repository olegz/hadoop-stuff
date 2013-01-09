package oz.poc.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.context.SmartLifecycle;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.message.GenericMessage;
/**
 * 
 * @author oleg
 *
 */
public class TailF implements SmartLifecycle{
	
	private static final Logger logger = Logger.getLogger(TailF.class);
	
	private final static int defaultTaskSubmissionDelay = 1000;
	
	private volatile ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
	
	private volatile int taskSubmissionDelay;
	
	private volatile File file;
	
	private boolean running;
	
	private final MessageChannel channel;
	
	private int recordSentCounter;
	
	/**
	 * 
	 * @param file
	 */
	public TailF(File file, MessageChannel channel){
		this(file, channel, defaultTaskSubmissionDelay);
	}
	
	/**
	 * 
	 * @param file
	 * @param corePoolSize
	 * @param taskSubmissionDelay
	 */
	public TailF(File file, MessageChannel channel, int taskSubmissionDelay){
		if (file == null){
			throw new IllegalStateException("'file' must not be null");
		}
		this.taskSubmissionDelay = taskSubmissionDelay;
		this.file = file;
		this.channel = channel;
	}
	
	/**
	 * 
	 */
	public void start(){
		this.running = true;
		this.scheduler.submit(new WaitingForFileTask());
	}
	
	/**
	 * 
	 */
	public void stop(){
		this.scheduler.shutdown();
		this.running = false;
	}
	
	/**
	 * 
	 * @author oleg
	 *
	 */
	private class WaitingForFileTask implements Runnable {
		
		private final FileReadingTask fileReadingTask = new FileReadingTask();

		@Override
		public void run() {
			System.out.println("Submitting");
			if (!file.exists()){
				System.out.println("Waiting for file: " + file.getAbsolutePath());
				scheduler.schedule(this, taskSubmissionDelay, TimeUnit.MILLISECONDS);
			}	
			else {
				try {
					scheduler.submit(fileReadingTask);
				} catch (Exception e) {
					e.printStackTrace();
				}		
			}
		}	
	}
	
	private class FileReadingTask implements Runnable {

		@Override
		public void run() {
			try {

				FileTime fileTime = this.getFileCreationTime(file);
				
				BufferedReader br = new BufferedReader(new FileReader(file));
				long filePointer = 0;
				long fileLength = file.length();
				while(running) {
					
					if (fileLength <= filePointer && this.getFileCreationTime(file).compareTo(fileTime) != 0) {
						System.out.println(fileTime);
						System.out.println("Closing");
						br.close();
						
						br = new BufferedReader(new FileReader(file));
						filePointer = 0;
						fileLength = file.length();
					}
					else {
						String strLine = br.readLine();
						if (strLine != null){
							if (strLine.contains("126.247.0.97")){
								Thread.sleep(new Random().nextInt(2000));
							}
							if (!channel.send(new GenericMessage<String>(strLine), 1000)){
								logger.warn("Failed to send a record: " + strLine);
							}
							recordSentCounter++;
							if (recordSentCounter % 500000 == 0){
								Thread.sleep(new Random().nextInt(1000));
								logger.info("Sent " + recordSentCounter + " records");
								
							}
							filePointer += (strLine + "\n").length();
							if (recordSentCounter == 20000000){
								System.out.println(filePointer + " - " + fileLength);
							}
						}	
						else {
							Thread.sleep(100);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				scheduler.schedule(this, taskSubmissionDelay, TimeUnit.MILLISECONDS);
			}
		}
		private FileTime getFileCreationTime(File theFile) throws Exception{
			Path path = FileSystems.getDefault().getPath(theFile.getAbsolutePath());
			BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
			return bfa.creationTime();
		}
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		this.stop();
	}
	

}

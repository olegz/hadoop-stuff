package oz.poc.mapred;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class LoggerTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("config.xml", LoggerTest.class);
		RecordLogger recordLogger = context.getBean(RecordLogger.class);

		while (true) {
			recordLogger.log("Hello UDP logging");
			System.out.println("Logging to UDP");
			Thread.sleep(2000);
		}
	}
	
	public static interface RecordLogger {
		public void log(String record);
	}

}
